package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	resty "github.com/go-resty/resty/v2"
	"github.com/samber/lo"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

const DEBUG = false

func main() {
	outDir := "out"
	if DEBUG {
		outDir = "debug_out"
	}
	err := os.MkdirAll(outDir, 0755)
	if err != nil {
		logger.Fatal(err)
	}
	projectID := os.Getenv("TRACKER_PROJECT")
	trackerToken := os.Getenv("TRACKER_TOKEN")
	if projectID == "" || trackerToken == "" {
		logger.Fatalf("supply TRACKER_PROJECT and TRACKER_TOKEN as env variables first")
	}

	client := resty.New()
	client.SetBaseURL("https://www.pivotaltracker.com/services/v5")
	client.Header.Set("X-TrackerToken", trackerToken)

	client.SetPathParam("project_id", projectID)

	d := Downloader{
		client: client,
	}
	cacheFile := filepath.Join(outDir, "cache.json")
	d.load(cacheFile)

	handleInterruptSignals(func() {
		d.cancelRequested = true
		logger.Println("cancel requested, stopping...")
	})

	visitList := func(path string, keys PathKeys, downstream func([]genericJSON)) {
		visit[[]genericJSON](&d, path, keys, fetchSimple, downstream)
	}
	_ = visitList
	visitObject := func(path string, keys PathKeys, downstream func(genericJSON)) {
		visit[genericJSON](&d, path, keys, fetchSimple, downstream)
	}

	pathkeys := PathKeys{}.withKey("project_id", projectID)
	visitObject("/projects/{project_id}", pathkeys, nil)
	d.visitPaginated("/projects/{project_id}/activity", pathkeys, nil)
	visitList("/projects/{project_id}/labels", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("label_id", getNumericKey(item, "id"))
		visitObject("/projects/{project_id}/labels/{label_id}", pathkeys, nil)
	}))
	visitList("/projects/{project_id}/memberships", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("membership_id", getNumericKey(item, "id"))
		visitObject("/projects/{project_id}/memberships/{membership_id}", pathkeys, nil)
	}))
	d.visitPaginated("/projects/{project_id}/releases", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("id", getNumericKey(item, "id"))
		visitObject("/projects/{project_id}/releases/{id}", pathkeys, nil)
		visitList("/projects/{project_id}/releases/{id}/stories", pathkeys, nil)
	}))
	d.visitPaginated("/projects/{project_id}/iterations", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("iteration_number", getNumericKey(item, "number"))
		visitObject("/projects/{project_id}/iterations/{iteration_number}", pathkeys, nil)
	}))
	visitList("/projects/{project_id}/epics", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("epic_id", getNumericKey(item, "id"))
		visitObject("/projects/{project_id}/epics/{epic_id}", pathkeys, nil)
		d.visitPaginated("/projects/{project_id}/epics/{epic_id}/activity", pathkeys, nil)
		visitList("/projects/{project_id}/epics/{epic_id}/comments", pathkeys, foreach(func(item genericJSON) {
			pathkeys := pathkeys.withKey("comment_id", getNumericKey(item, "id"))
			visitObject("/projects/{project_id}/epics/{epic_id}/comments/{comment_id}", pathkeys, nil)
		}))

	}))
	d.visitPaginated("/projects/{project_id}/stories", pathkeys, foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("story_id", getNumericKey(item, "id"))
		visitObject("/projects/{project_id}/stories/{story_id}", pathkeys, nil)
		visitList("/projects/{project_id}/stories/{story_id}/owners", pathkeys, foreach(func(item genericJSON) {
			//pathkeys := pathkeys.withKey("person_id", getNumericKey(item, "id"))
			//visitObject("/projects/{project_id}/stories/{story_id}/owners/{person_id}", pathkeys, nil)
		}))
		visitList("/projects/{project_id}/stories/{story_id}/reviews", pathkeys, foreach(func(item genericJSON) {
			pathkeys := pathkeys.withKey("review_id", getNumericKey(item, "id"))
			visitObject("/projects/{project_id}/stories/{story_id}/reviews/{review_id}", pathkeys, nil)
		}))
		visitList("/projects/{project_id}/stories/{story_id}/tasks", pathkeys, foreach(func(item genericJSON) {
			pathkeys := pathkeys.withKey("task_id", getNumericKey(item, "id"))
			visitObject("/projects/{project_id}/stories/{story_id}/tasks/{task_id}", pathkeys, nil)
		}))
		visitList("/projects/{project_id}/stories/{story_id}/transitions", pathkeys, nil)
		visitList("/projects/{project_id}/stories/{story_id}/activity", pathkeys, nil)
		visitList("/projects/{project_id}/stories/{story_id}/blockers", pathkeys, foreach(func(item genericJSON) {
			pathkeys := pathkeys.withKey("blocker_id", getNumericKey(item, "id"))
			visitObject("/projects/{project_id}/stories/{story_id}/blockers/{blocker_id}", pathkeys, nil)
		}))
		visitList("/projects/{project_id}/stories/{story_id}/comments", pathkeys, foreach(func(item genericJSON) {
			pathkeys := pathkeys.withKey("comment_id", getNumericKey(item, "id"))
			visitObject("/projects/{project_id}/stories/{story_id}/comments/{comment_id}", pathkeys, nil)
		}))
		visitList("/projects/{project_id}/stories/{story_id}/labels", pathkeys, foreach(func(item genericJSON) {
			//pathkeys := pathkeys.withKey("label_id", getNumericKey(item, "id"))
			// giving 404?
			// visitObject("/projects/{project_id}/stories/{story_id}/labels/{label_id}", pathkeys, nil)
		}))
	}))
	completionChecker.report()
	d.save(cacheFile)
	d.dumpCopies(outDir)
	logger.Println("done")
}

func foreach(f func(item genericJSON)) func(itemList []genericJSON) {
	return func(itemList []genericJSON) {
		for _, item := range itemList {
			f(item)
		}
	}
}

func handleInterruptSignals(done func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		_ = <-sigs
		done()
	}()
}

type PathKeys map[string]string

func (pk PathKeys) withKey(key, value string) PathKeys {
	result := PathKeys{}
	for k, v := range pk {
		result[k] = v
	}
	result[key] = value
	return result
}

func (pk PathKeys) equal(keys PathKeys) bool {
	if len(pk) != len(keys) {
		return false
	}
	for k, v := range pk {
		if v != keys[k] {
			return false
		}
	}
	return true
}

type DownloadedContent struct {
	PathTemplate string
	Keys         PathKeys
	Data         any
}
type Downloader struct {
	downloadedData  []*DownloadedContent
	cancelRequested bool
	client          *resty.Client
}

func (d *Downloader) visitPaginated(path string, keys PathKeys, downstreamHandler func([]genericJSON)) {
	visit[[]genericJSON](d, path, keys, fetchPaginated, downstreamHandler)
}

func visit[T any](d *Downloader, path string, keys PathKeys, fetcher func(req *resty.Request, path string) T, downstreamHandler func(resp T)) {
	if d.cancelRequested {
		return
	}

	var resp T
	isCached := false
	for _, cached := range d.downloadedData {
		if cached.PathTemplate == path && cached.Keys.equal(keys) {
			recodeJsonAs(cached.Data, &resp)
			isCached = true
			completionChecker.observe(cached.PathTemplate)
			break
		}
	}
	if !isCached {
		resp = fetcher(d.getRequest(keys), path)
		d.addResult(path, keys, resp)
	}

	if downstreamHandler != nil {
		downstreamHandler(resp)
	}
}

func recodeJsonAs[T any](data any, t *T) {
	marshalledContent, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(marshalledContent, t)
	if err != nil {
		panic(err)
	}
}

func (d *Downloader) addResult(requestTemplate string, keys PathKeys, response any) {
	d.downloadedData = append(d.downloadedData, &DownloadedContent{
		Keys:         keys,
		PathTemplate: requestTemplate,
		Data:         response,
	})
}

func (d *Downloader) getRequest(keys PathKeys) *resty.Request {
	req := d.client.R()
	for k, v := range keys {
		req.SetPathParam(k, v)
	}
	return req
}

func (d *Downloader) save(file string) {
	saveAsJSON(d.downloadedData, file)
}

func saveAsJSON(data any, file string) {
	res, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		logger.Fatal(err)
	}

	if err := os.WriteFile(file, res, 0644); err != nil {
		logger.Fatal(err)
	}

}

func (d *Downloader) load(file string) {
	contents, err := os.ReadFile(file)
	if err != nil {
		return
	}

	if err := json.Unmarshal(contents, &d.downloadedData); err != nil {
		logger.Fatal(err)
	}
}

func (d *Downloader) dumpCopies(outDir string) {
	groups := lo.GroupBy(d.downloadedData, func(item *DownloadedContent) string {
		return item.PathTemplate
	})

	nonAlphanumericRe := regexp.MustCompile("[^a-zA-Z0-9]+")
	for pathTemplate, group := range groups {
		fileName := nonAlphanumericRe.ReplaceAllString(pathTemplate, "_")
		contents := lo.Map(group, func(content *DownloadedContent, _ int) any {
			return content.Data
		})
		saveAsJSON(contents, filepath.Join(outDir, fileName))
	}
}

type completionEntry struct {
	expr     *regexp.Regexp
	seen     bool
	template string
}

// tracks if we processed all endpoints in allTrackerAPIEndpoints
type CompletionChecker struct {
	items []*completionEntry
}

func (c *CompletionChecker) observe(url string) {
	for _, i := range c.items {
		if i.seen {
			continue
		}
		if i.expr.FindStringIndex(url) != nil {
			logger.Println("completion checker: registering as observed: ", i.template)
			i.seen = true
			break
		}
	}
}

func (c *CompletionChecker) report() {
	done := true
	for _, i := range c.items {
		if i.seen {
			continue
		}
		logger.Println("completion checker: not done yet: ", i.template)
		done = false
	}
	if done {
		logger.Println("completion checker: all done.")
	}
}

func newCompletionChecker() *CompletionChecker {
	wantedAPIEndpoints := lo.Filter(allTrackerAPIEndpoints, func(item string, index int) bool {
		return !lo.Contains(ignoredEndpoints, item)
	})

	return &CompletionChecker{
		items: lo.Map(wantedAPIEndpoints, func(urlTemplate string, index int) *completionEntry {
			substitutePathParams := regexp.MustCompile(`({[^/]+)}`)
			urlRegexp := substitutePathParams.ReplaceAllString(urlTemplate, "[^/]+") + `($|\?.*)`

			return &completionEntry{
				expr:     regexp.MustCompile(urlRegexp),
				seen:     false,
				template: urlTemplate,
			}
		}),
	}
}

var completionChecker = newCompletionChecker()

type genericJSON map[string]any

func fetchPaginated(req *resty.Request, path string) []genericJSON {
	type paginatedResponse struct {
		Pagination struct {
			Total    int `json:"total"`
			Limit    int `json:"limit"`
			Offset   int `json:"offset"`
			Returned int `json:"returned"`
		} `json:"pagination"`
		Data []genericJSON `json:"data"`
	}

	var result []genericJSON
	done := false
	for !done {
		respBody := paginatedResponse{}

		req := req.
			SetResult(&respBody).
			SetQueryParam("offset", fmt.Sprintf("%d", len(result))).
			SetQueryParam("envelope", "true")

		getWithRetries(req, path)

		result = append(result, respBody.Data...)
		logger.Printf("fetched %d/%d\n", len(result), respBody.Pagination.Total)
		done = len(result) == respBody.Pagination.Total
		if DEBUG {
			break
		}
	}
	return result
}

func fetchSimple[T any](req *resty.Request, path string) T {
	var resp T
	req.SetResult(&resp)
	getWithRetries(req, path)
	return resp
}

func getWithRetries(req *resty.Request, path string) {
	var retryTimeout = time.Duration(0)
	for {
		resp, err := req.Get(path)
		if err != nil {
			log.Println("API call failed, will retry")
			time.Sleep(5 * time.Second)
			continue
		}
		logger.Printf("finished call: %s", req.URL)

		completionChecker.observe(req.URL)

		switch resp.StatusCode() {
		case http.StatusOK:
			return
		case http.StatusTooManyRequests:
			retryTimeout += 5 * time.Second
			logger.Printf("server complaining about too many requests, sleeping for %s and retrying in a bit", retryTimeout)
			time.Sleep(retryTimeout)

		default:
			logger.Fatalf("unexpected status (%s): %v", req.RawRequest.URL.String(), resp.Status())
		}
	}
}

func getNumericKey(obj genericJSON, key string) string {
	return fmt.Sprintf("%d", int64(obj[key].(float64)))
}
