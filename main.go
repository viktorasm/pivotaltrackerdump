package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	handleInterruptSignals(func() {
		d.cancelRequested = true
		logger.Println("cancel requested, stopping...")
	})

	pathkeys := PathKeys{}.withKey("project_id", projectID)
	d.visitPaginated("/projects/{project_id}/stories", pathkeys, func(resp []genericJSON) {
		for _, respItem := range resp {
			pathkeys := pathkeys.withKey("story_id", getNumericKey(respItem, "id"))
			d.visitObject("/projects/{project_id}/stories/{story_id}", pathkeys, nil)
		}
	})
	completionChecker.report()
	logger.Println("done")
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

type DownloadedContent struct {
	pathTemplate string
	keys         PathKeys
	PathKeys     PathKeys
	Data         any
}
type Downloader struct {
	downloadedData  []DownloadedContent
	cancelRequested bool
	client          *resty.Client
}

func (d *Downloader) visitPaginated(path string, keys PathKeys, downstreamHandler func([]genericJSON)) {
	if d.cancelRequested {
		return
	}

	contents := fetchPaginated(d.getRequest(keys), path)
	d.addResult(path, keys, contents)

	downstreamHandler(contents)
}

func (d *Downloader) visitList(path string, keys PathKeys, downstreamHandler func(resp []genericJSON)) {
	if d.cancelRequested {
		return
	}
	var resp []genericJSON

	getWithRetries(d.getRequest(keys), path)
	d.addResult(path, keys, resp)
	downstreamHandler(resp)
}

func (d *Downloader) visitObject(path string, keys PathKeys, downstreamHandler func(resp genericJSON)) {
	if d.cancelRequested {
		return
	}
	var resp genericJSON

	getWithRetries(d.getRequest(keys), path)
	d.addResult(path, keys, resp)

	if downstreamHandler != nil {
		downstreamHandler(resp)
	}
}

func (d *Downloader) addResult(requestTemplate string, keys PathKeys, response any) {
	d.downloadedData = append(d.downloadedData, DownloadedContent{
		keys:         keys,
		pathTemplate: requestTemplate,
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

/*
func persistJson[T any](fetcher ContentsFetcher[T], destinationFile string) ContentsFetcher[T] {
	return func() *FetchedContents[T] {
		existingFileContents, err := os.ReadFile(destinationFile)
		if err == nil {
			var contents FetchedContents[T]

			err := json.Unmarshal(existingFileContents, &contents)
			if err == nil {
				logger.Printf("file %q exists, reusing cached contents", destinationFile)
				for _, src := range contents.Sources {
					completionChecker.observe(src)
				}
				return &contents
			}
		}

		logger.Println("fetching contents for", destinationFile)
		contents := fetcher()

		marshalledContents, _ := json.MarshalIndent(contents, "", "  ")

		if err := os.WriteFile(destinationFile, marshalledContents, 0644); err != nil {
			log.Fatalf("error writing file %q: %v", destinationFile, err)
		}
		logger.Println("file created:", destinationFile)
		return contents
	}
}


type requestFn func(*resty.Request)

func withPathParam(key string, value string) requestFn {
	return func(r *resty.Request) {
		r.SetPathParam(key, value)
	}
}

func fetchSimple[T any](client *resty.Client, path string, requestFn ...requestFn) *FetchedContents[T] {
	var respBody T

	req := client.R().SetResult(&respBody)

	result := FetchedContents[T]{}
	getWithRetries(&result, req, path, requestFn...)

	result.Sources = []string{req.URL}
	result.Data = respBody
	result.SavedDate = time.Now()

	return &result
}

type Subcontent[T any] struct {
	ParentIDs parentIDs
	Data      T
}



func getSubcontent[T any](client *resty.Client, path string, parents []parentIDs, parentParams ...string) *FetchedContents[[]Subcontent[T]] {
	//goland:noinspection GoBoolExpressions
	if DEBUG && len(parents) > 3 {
		parents = parents[:3]
	}

	result := FetchedContents[[]Subcontent[T]]{
		SavedDate: time.Now(),
	}

	result.Data = lo.Map(parents, func(parentKeyValues parentIDs, index int) Subcontent[T] {
		fmt.Printf("\rfetching items (%d/%d), parent IDs %v", index+1, len(parents), spew.NewFormatter(parentKeyValues))

		params := lo.Map(parentParams, func(key string, index int) requestFn {
			return withPathParam(key, parentKeyValues[index])
		})
		subcontent := fetchSimple[T](client, path, params...)

		result.Sources = append(result.Sources, subcontent.Sources...)

		return Subcontent[T]{
			ParentIDs: parentKeyValues,
			Data:      subcontent.Data,
		}
	})
	return &result
}



*/
