package main

import (
	"encoding/json"
	"fmt"
	resty "github.com/go-resty/resty/v2"
	"github.com/samber/lo"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
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
	client.SetBaseURL("https://www.pivotaltracker.com/services/v5/projects/" + projectID)
	client.Header.Set("X-TrackerToken", trackerToken)

	simpleList := func(path string) ContentsFetcher[[]genericJSON] {
		return func() *FetchedContents[[]genericJSON] {
			return fetchSimple[[]genericJSON](client, path)
		}
	}
	paginatedList := func(path string) ContentsFetcher[[]genericJSON] {
		return func() *FetchedContents[[]genericJSON] {
			return fetchPaginated(client, path)
		}
	}
	sublistByKey := func(parents []genericJSON, parentKey string, path string, parentParam string) ContentsFetcher[[]Subcontent[[]genericJSON]] {
		return func() *FetchedContents[[]Subcontent[[]genericJSON]] {
			return getSubcontent[[]genericJSON](client, parents, path, parentKey, parentParam)
		}
	}
	subObjectByKey := func(parents []genericJSON, parentKey string, path string, pathParam string) ContentsFetcher[[]Subcontent[genericJSON]] {
		return func() *FetchedContents[[]Subcontent[genericJSON]] {
			return getSubcontent[genericJSON](client, parents, path, parentKey, pathParam)
		}
	}
	sublist := func(parents []genericJSON, path string, parentParam string) ContentsFetcher[[]Subcontent[[]genericJSON]] {
		return sublistByKey(parents, "id", path, parentParam)
	}
	out := func(file string) string {
		return filepath.Join(outDir, file)
	}

	{
		var stories []genericJSON
		capture(persistJson(paginatedList("/stories"), out("stories.json")), &stories)()
		persistJson(subObjectByKey(stories, "id", "/stories/{storyID}", "storyID"), out("story_details.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/tasks", "storyID"), out("story_tasks.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/owners", "storyID"), out("story_owners.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/comments", "storyID"), out("story_comments.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/reviews", "storyID"), out("story_reviews.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/blockers", "storyID"), out("story_blockers.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/transitions", "storyID"), out("story_transitions.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/activity", "storyID"), out("story_activity.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/labels", "storyID"), out("story_labels.json"))()
	}

	{
		var iterations []genericJSON
		capture(persistJson(paginatedList("/iterations"), out("iterations.json")), &iterations)()
		// analytics are fetchable only from last 6 months. Ignore? contents not very useful
		// persistJson(sublistByKey(iterations, "/iterations/{iteration_number}/analytics", "number", "iteration_number"), out("iteration_analytics.json"))()
	}

	{
		var releases []genericJSON
		capture(persistJson(paginatedList("/releases"), out("releases.json")), &releases)()
		persistJson(sublist(releases, "/releases/{id}/stories", "id"), out("releases_stories.json"))()
		persistJson(simpleList("/labels"), out("labels.json"))()
	}

	{
		var epics []genericJSON
		capture(persistJson(simpleList("/epics"), out("epics.json")), &epics)()
		persistJson(sublist(epics, "/epics/{epic_id}/comments", "epic_id"), out("epic_comments.json"))()
		persistJson(sublist(epics, "/epics/{epic_id}/activity", "epic_id"), out("epic_activity.json"))()
	}

	persistJson(simpleList("/memberships"), out("memberships.json"))()
	persistJson(paginatedList("/activity"), out("activity.json"))()

	completionChecker.report()
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

type FetchedContents[T any] struct {
	Sources   []string
	SavedDate time.Time
	Data      T
}

type ContentsFetcher[T any] func() *FetchedContents[T]

func capture[T any](fetcher ContentsFetcher[T], cache *T) ContentsFetcher[T] {
	return func() *FetchedContents[T] {
		results := fetcher()
		*cache = results.Data
		return results
	}
}

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

type genericJSON map[string]any

func fetchPaginated(client *resty.Client, path string) *FetchedContents[[]genericJSON] {
	type paginatedResponse struct {
		Pagination struct {
			Total    int `json:"total"`
			Limit    int `json:"limit"`
			Offset   int `json:"offset"`
			Returned int `json:"returned"`
		} `json:"pagination"`
		Data []genericJSON `json:"data"`
	}

	var result FetchedContents[[]genericJSON]
	done := false
	for !done {
		respBody := paginatedResponse{}

		getWithRetries(&result, client.R().
			SetResult(&respBody).
			SetQueryParam("offset", fmt.Sprintf("%d", len(result.Data))).
			SetQueryParam("envelope", "true"), path)

		result.Data = append(result.Data, respBody.Data...)
		logger.Printf("fetched %d/%d\n", len(result.Data), respBody.Pagination.Total)
		done = len(result.Data) == respBody.Pagination.Total
		if DEBUG {
			break
		}
	}
	return &result
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
	ParentID int64
	Data     T
}

func getSubcontent[T any](client *resty.Client, parents []genericJSON, path string, parentKey, parentParam string) *FetchedContents[[]Subcontent[T]] {
	//goland:noinspection GoBoolExpressions
	if DEBUG && len(parents) > 3 {
		parents = parents[:3]
	}

	result := FetchedContents[[]Subcontent[T]]{
		SavedDate: time.Now(),
	}

	result.Data = lo.Map(parents, func(parent genericJSON, index int) Subcontent[T] {
		parentID := int64(parent[parentKey].(float64))
		fmt.Printf("\rfetching items (%d/%d), parent ID %d", index+1, len(parents), parentID)

		subcontent := fetchSimple[T](client, path, withPathParam(parentParam, strconv.FormatInt(parentID, 10)))

		result.Sources = append(result.Sources, subcontent.Sources...)

		return Subcontent[T]{
			ParentID: parentID,
			Data:     subcontent.Data,
		}
	})
	return &result
}

func getWithRetries[T any](result *FetchedContents[T], req *resty.Request, path string, requestFn ...requestFn) {
	for _, fn := range requestFn {
		fn(req)
	}
	var retryTimeout = time.Duration(0)
	for {
		resp, err := req.Get(path)
		if err != nil {
			log.Println("API call failed, will retry")
			time.Sleep(5 * time.Second)
			continue
		}

		completionChecker.observe(req.URL)
		result.Sources = append(result.Sources, resp.Request.URL)

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
