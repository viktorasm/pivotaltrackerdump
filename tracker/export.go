package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/samber/lo"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

func Export(ctx context.Context, trackerToken string, projectID string, outDir string) {
	client := resty.New()
	client.SetBaseURL("https://www.pivotaltracker.com/services/v5")
	client.Header.Set("X-TrackerToken", trackerToken)

	client.SetPathParam("project_id", projectID)

	d := Downloader{
		client: client,
	}
	cacheFile := filepath.Join(outDir, "cache.json")
	d.load(cacheFile)

	visitList := func(path string, keys PathKeys, fields string, downstream func([]genericJSON)) {
		visit[[]genericJSON](ctx, &d, path, keys, fields, fetchSimple, downstream)
	}
	_ = visitList
	visitObject := func(path string, keys PathKeys, fields string, downstream func(genericJSON)) {
		visit[genericJSON](ctx, &d, path, keys, fields, fetchSimple, downstream)
	}
	_ = visitObject

	pathkeys := PathKeys{}.withKey("project_id", projectID)
	visitObject("/projects/{project_id}", pathkeys, "", nil)
	d.visitPaginated(ctx, "/projects/{project_id}/activity", pathkeys, 10, "", nil)
	visitList("/projects/{project_id}/labels", pathkeys, "", nil)
	visitList("/projects/{project_id}/memberships", pathkeys, "", nil)
	d.visitPaginated(ctx, "/projects/{project_id}/releases", pathkeys, 10, ":default,story_ids", nil)
	d.visitPaginated(ctx, "/projects/{project_id}/iterations", pathkeys, 10, "", nil)
	visitList("/projects/{project_id}/epics", pathkeys, ":default,comments(:default,file_attachments,google_attachments,attachment_ids)", foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("epic_id", getNumericKey(item, "id"))
		d.handleCommentAttachments(item)
		visitList("/projects/{project_id}/epics/{epic_id}/activity", pathkeys, "", nil)
	}))
	d.visitPaginated(ctx, "/projects/{project_id}/stories", pathkeys, 10, ":default,comments(:default,file_attachments,google_attachments,attachment_ids),owners(:default),reviews(:default),tasks(:default),transitions(:default),blockers(:default),labels(:default)", foreach(func(item genericJSON) {
		pathkeys := pathkeys.withKey("story_id", getNumericKey(item, "id"))

		d.handleCommentAttachments(item)
		visitList("/projects/{project_id}/stories/{story_id}/activity", pathkeys, "", nil)
	}))
	completionChecker.report()
	d.save(cacheFile)
	d.dumpCopies(outDir)
}

func foreach(f func(item genericJSON)) func(itemList []genericJSON) {
	return func(itemList []genericJSON) {
		for _, item := range itemList {
			f(item)
		}
	}
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
	downloadedData []*DownloadedContent
	client         *resty.Client
}

func (d *Downloader) visitPaginated(ctx context.Context, path string, keys PathKeys, pageLimit int, fields string, downstreamHandler func([]genericJSON)) {
	visit[[]genericJSON](ctx, d, path, keys, fields, makePaginatedFetcher(pageLimit), downstreamHandler)
}

type Fetcher[T any] func(ctx context.Context, req *resty.Request, path string) (T, error)

func visit[T any](ctx context.Context, d *Downloader, path string, keys PathKeys, fields string, fetcher Fetcher[T], downstreamHandler func(resp T)) {
	select {
	case <-ctx.Done():
		return
	default:

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
		var err error
		resp, err = fetcher(ctx, d.getRequest(keys, fields), path)
		if err != nil {
			logger.Println(err.Error())
			return
		}

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

func (d *Downloader) getRequest(keys PathKeys, fields string) *resty.Request {
	req := d.client.R()
	for k, v := range keys {
		req.SetPathParam(k, v)
	}
	if fields != "" {
		req.SetQueryParam("fields", fields)
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
		fileName := strings.Trim(nonAlphanumericRe.ReplaceAllString(pathTemplate, "_"), "_") + ".json"
		contents := lo.Map(group, func(content *DownloadedContent, _ int) any {
			return content.Data
		})
		saveAsJSON(contents, filepath.Join(outDir, fileName))
	}
}

func (d *Downloader) handleCommentAttachments(itemWithComments genericJSON) {
	for _, comment := range getListFieldValue(itemWithComments, "comments") {
		for _, attachment := range getListFieldValue(comment, "file_attachments") {
			filename, ok := getFieldValue(attachment, "filename")
			if !ok {
				log.Fatalf("could not get attachment filename")
			}
			downloadUrl, ok := getFieldValue(attachment, "download_url")
			if !ok {
				log.Fatalf("could not get attachment download url")
			}
			attachmentID, ok := getFieldValue(attachment, "id")
			if !ok {
				log.Fatalf("could not get attachment id")
			}
			attachmentIDStr := fmt.Sprintf("%d", int64(attachmentID.(float64)))
			logger.Printf("attachment detected: %q %q %v", filename, downloadUrl, attachmentIDStr)
		}
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

func getFieldValue(obj any, key string) (any, bool) {
	value := reflect.ValueOf(obj).MapIndex(reflect.ValueOf(key))
	if value.IsValid() {
		return value.Interface(), true
	}
	return nil, false
}

func getListFieldValue(obj any, key string) []any {
	value, ok := getFieldValue(obj, key)
	if !ok {
		return nil
	}
	listValue, ok := value.([]any)
	if !ok {
		return nil
	}
	return listValue
}

func makePaginatedFetcher(limit int) func(ctx context.Context, req *resty.Request, path string) ([]genericJSON, error) {
	return func(ctx context.Context, req *resty.Request, path string) ([]genericJSON, error) {
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
				SetContext(ctx).
				SetResult(&respBody).
				SetQueryParam("limit", fmt.Sprintf("%d", limit)).
				SetQueryParam("offset", fmt.Sprintf("%d", len(result))).
				SetQueryParam("envelope", "true")

			err := getWithRetries(ctx, req, path)
			if err != nil {
				return nil, err
			}

			result = append(result, respBody.Data...)
			logger.Printf("fetched %d/%d\n", len(result), respBody.Pagination.Total)
			done = len(result) == respBody.Pagination.Total
		}
		return result, nil
	}
}

func fetchSimple[T any](ctx context.Context, req *resty.Request, path string) (T, error) {
	var resp T
	req.SetContext(ctx)
	req.SetResult(&resp)
	err := getWithRetries(ctx, req, path)
	return resp, err
}

func getWithRetries(ctx context.Context, req *resty.Request, path string) error {
	req.SetContext(ctx)
	var retryTimeout = time.Duration(0)
	for {
		req.SetContext(ctx)
		resp, err := req.Get(path)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Println("API call failed, will retry")
			time.Sleep(5 * time.Second)
			continue
		}
		logger.Printf("finished call: %s", req.URL)

		completionChecker.observe(req.URL)

		switch resp.StatusCode() {
		case http.StatusOK:
			return nil
		case http.StatusTooManyRequests:
			retryTimeout += 5 * time.Second
			logger.Printf("server complaining about too many requests, sleeping for %s", retryTimeout)
		default:
			logger.Fatalf("unexpected status (%s): %v", req.RawRequest.URL.String(), resp.Status())
		}

		select {
		case <-time.After(retryTimeout):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func getNumericKey(obj genericJSON, key string) string {
	return fmt.Sprintf("%d", int64(obj[key].(float64)))
}
