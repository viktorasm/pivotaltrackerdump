package main

import (
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	resty "github.com/go-resty/resty/v2"
	"github.com/samber/lo"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
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

	simpleObject := func(path string) ContentsFetcher[genericJSON] {
		return func() *FetchedContents[genericJSON] {
			return fetchSimple[genericJSON](client, path)
		}
	}
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
	sublistByKey := func(path string, parentIDs []parentIDs, parentParam ...string) ContentsFetcher[[]Subcontent[[]genericJSON]] {
		return func() *FetchedContents[[]Subcontent[[]genericJSON]] {
			return getSubcontent[[]genericJSON](client, path, parentIDs, parentParam...)
		}
	}
	subObjectByKey := func(path string, parentIDs []parentIDs, pathParam ...string) ContentsFetcher[[]Subcontent[genericJSON]] {
		return func() *FetchedContents[[]Subcontent[genericJSON]] {
			return getSubcontent[genericJSON](client, path, parentIDs, pathParam...)
		}
	}
	parentIDs := func(parents []genericJSON, key string) []parentIDs {
		return lo.Map(parents, func(item genericJSON, _ int) parentIDs {
			return parentIDs{getNumericKey(item, key)}
		})
	}
	sublist := func(parents []genericJSON, path string, parentParam string) ContentsFetcher[[]Subcontent[[]genericJSON]] {
		return sublistByKey(path, parentIDs(parents, "id"), parentParam)
	}
	out := func(file string) string {
		return filepath.Join(outDir, file)
	}

	persistJson(simpleObject(""), out("project.json"))()
	{
		var stories []genericJSON
		var storycomments []Subcontent[[]genericJSON]

		capture(persistJson(paginatedList("/stories"), out("stories.json")), &stories)()
		persistJson(subObjectByKey("/stories/{storyID}", parentIDs(stories, "id"), "storyID"), out("story_details.json"))()
		capture(persistJson(sublist(stories, "/stories/{storyID}/comments", "storyID"), out("story_comments.json")), &storycomments)()
		persistJson(subObjectByKey("/stories/{storyID}/comments/{comment_id}", nestedParentKeys(storycomments), "storyID", "comment_id"), out("story_comment_details.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/tasks", "storyID"), out("story_tasks.json"))()
		persistJson(sublist(stories, "/stories/{storyID}/owners", "storyID"), out("story_owners.json"))()
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
		persistJson(subObjectByKey("/releases/{id}", parentIDs(releases, "id"), "id"), out("release_details.json"))()
		persistJson(sublist(releases, "/releases/{id}/stories", "id"), out("releases_stories.json"))()
		persistJson(simpleList("/labels"), out("labels.json"))()
	}

	{
		var epics []genericJSON
		capture(persistJson(simpleList("/epics"), out("epics.json")), &epics)()
		persistJson(subObjectByKey("/epics/{id}", parentIDs(epics, "id"), "id"), out("epic_details.json"))()
		var epiccomments []Subcontent[[]genericJSON]
		capture(persistJson(sublist(epics, "/epics/{epic_id}/comments", "epic_id"), out("epic_comments.json")), &epiccomments)()
		persistJson(sublistByKey("/epics/{epic_id}/comments/{comment_id}", nestedParentKeys(epiccomments), "epic_id", "comment_id"), out("epic_comments_details.json"))()
		persistJson(sublist(epics, "/epics/{epic_id}/activity", "epic_id"), out("epic_activity.json"))()
	}

	persistJson(simpleList("/memberships"), out("memberships.json"))()
	persistJson(paginatedList("/activity"), out("activity.json"))()

	completionChecker.report()
}

type parentIDs []string

func nestedParentKeys(subcontent []Subcontent[[]genericJSON]) []parentIDs {
	result := []parentIDs{}
	for _, sub := range subcontent {
		for _, obj := range sub.Data {
			keys := append(parentIDs{}, sub.ParentIDs...)
			keys = append(keys, getNumericKey(obj, "id"))
			result = append(result, keys)
		}
	}
	return result

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
	ParentIDs parentIDs
	Data      T
}

func getNumericKey(obj genericJSON, key string) string {
	return fmt.Sprintf("%d", int64(obj[key].(float64)))
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
