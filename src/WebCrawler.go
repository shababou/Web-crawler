package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	. "UrlCrawling"
	. "Utilities"
)

const PORT = ":8080"
const STATUS = "status"
const RESULT = "result"



/* Job definition as per added in the entry point:
- job_id: unique id of the job,
- urls: Job URLs,
- workers: specified number of workers*/
type JobDef struct {
	Job_id string `json:"job_id"`
	Urls []string `json:"urls"`
	NbWorkers int `json:"workers"`
}

/* Job status with number of completed and in_progress Job URLs */
type JobStatus struct {
	sync.Mutex
	Completed int `json:"completed"`
	InProgress int `json:"in_progress"`
}

/* Result images per Job URL */
type JobResult map[string][]string 

/* Information during Job processing:
- urlsProcesses: information related (keys) to each Job URLs (keys) crawling process.*/
type JobProcess struct {
	urlsProcesses map[string]*UrlProcess
}

/* Job definition with all its data:
- Def: as per the JSON of the adding Job entry point,
- Process: information during job processing,
- Stats: number of completed and in-process URLs,
- Result: images per Job URL.*/
type Job struct {
	Def *JobDef
	Process *JobProcess
	Status *JobStatus
	Result *JobResult
}

/* Map of all the jobs (values) defined by their unique job_id (keys) */
type Jobs struct {
	jobs map[string]*Job
}



/* Updating job summary.
This method shall update the status and result parameters of the receiver specified job:
- the status shall consist in the number of in_progress Job URLs and completed Job URLs.
A Job URL shall be considered as completed when no more waiting URLs neither processing URLs related to this URL. The Job URL shall be considered as in_progress otherwise.
- the result shall consist in listing all unique data (images) retrieved from the crawling process for each of the Job URLs.
*/
func (job *Job) UpdateJobStatus() {
	// Retrieving the process information for each Job URL
	urlsProcesses := job.Process.urlsProcesses

	completed := 0
	inProgress := 0
	// Looping on each Job URL from the receiver specified job, and accessing to its process information
	for jobUrl, jobUrlProcess := range urlsProcesses {
		waitingUrls := jobUrlProcess.WaitingUrls
		processingUrls := jobUrlProcess.ProcessingUrls
		crawledUrls := jobUrlProcess.CrawledUrls
		waitingUrls.Lock()
		processingUrls.Lock()
		crawledUrls.Lock()

		if( (len(waitingUrls.Urls) == 0) && (len(processingUrls.UrlsData) == 0) ) {
			// Considering a Job URL completed when no more waiting URLs neither processing URLs from this URL 
			completed++
		} else {
			// Considering a Job URL as progressing if previous condition not fullfilled 
			inProgress++
		}

		completedData := []string{}
		// Appending data from each crawled URLs for the Job URL
		for _, urlsData := range crawledUrls.UrlsData {
			for data, _ := range urlsData {
	        	completedData = append(completedData, data)
	    	} 
	    }
	     // Removing data duplicates
      	completedData = RemoveSliceDuplicates(completedData)
      	(*job.Result)[jobUrl] = completedData

      	waitingUrls.Unlock()
		processingUrls.Unlock()
		crawledUrls.Unlock()
    }

    // Setting Job status
    job.Status.Completed = completed
    job.Status.InProgress = inProgress
 }


/* Job worker in action.
This method shall define the work cycle of the worker specified by its workerId as following:
1- The worker shall iterate through the Job URLs defined in the specified receiver job (Urls from Def parameter) and select the first URL available in the related waiting URLs set.
2- The worker shall select the first available URL by:  
	- removing this URL from the waiting URLs set,
	- adding this URL to the processing URLs set,
	- crawling the selected URL,
	- adding this URL and its data to the crawled URLs set,
	- removing this URL from the processing URLs set.
   Else, the worker shall:
	- update the specified receiver job,
	- leave the job only if all the Job URLs are completed; looking for a new URL to crawl else.
*/
func (job *Job) WorkOnJob(workerId int, wgJob *sync.WaitGroup) {
	// Giving a name to the worker for traces
	workerName := "Job_" + job.Def.Job_id + "_worker" + strconv.Itoa(workerId)

	jobProcess := job.Process

	// Working...
	WorkingLoop: 
	for{

		// For each Job URL defined in the specified receiver job...
		for jobUrl, jobUrlProcess := range jobProcess.urlsProcesses {
			waitingUrls := jobUrlProcess.WaitingUrls
			// Looking for a URL waiting to be crawled
			waitingUrls.Lock()
			fmt.Println(workerName + " looking for a task in URL: " + jobUrl + "\n")
	        if(len(waitingUrls.Urls) > 0){
	        	// At least one URL ready to be crawled
	        	// Selecting the first available URL to crawl 
	        	for waitingUrl, _ := range waitingUrls.Urls {
	        		fmt.Println(workerName + " getting ready to crawl URL: " + waitingUrl + "\n")
	        		// Removing the selected URL from the waiting URLs set since ready to be crawled by the worker
    				delete(waitingUrls.Urls, waitingUrl)
			        waitingUrls.Unlock()
			        // Adding the selected URL to the processing URLs set
			        processingUrls := jobUrlProcess.ProcessingUrls
			        processingUrls.Lock()
			        processingUrls.UrlsData[waitingUrl] = map[string]string{}
			        processingUrls.Unlock()

			        // Performing URL crawling
			        fmt.Println(workerName + " crawling URL: " + waitingUrl + " ...\n")
			        jobUrlProcess.CrawlUrl(&waitingUrl)

			        // Ending URL crawling
			        fmt.Println(workerName + " completed crawling URL: " + waitingUrl + "\n")
			        // Adding the crawled URL to the crawled URLs set
			        crawledUrls := jobUrlProcess.CrawledUrls
			        crawledUrls.Lock()
			        crawledUrls.UrlsData[waitingUrl] = processingUrls.UrlsData[waitingUrl]
			        crawledUrls.Unlock()
			        // Removing the crawled URL from the processing URLs set
			        processingUrls.Lock()
			        delete(processingUrls.UrlsData, waitingUrl)
			        processingUrls.Unlock()

			        fmt.Println(workerName + " finished task on URL: " + waitingUrl + "\n")

			        // Looking back for a new task, new URL to crawl...
			        goto WorkingLoop
		        	
		        }
	        }
	        waitingUrls.Unlock()
	    }
	    // updating the status of the specified receiver job
	    job.Status.Lock()
	    job.UpdateJobStatus()
	    if(job.Status.Completed == len(job.Def.Urls)){
	    	// Leaving the job if all the Job URLs are completed
	    	job.Status.Unlock()
	    	break WorkingLoop
	    }
	    job.Status.Unlock()
	    
	}

	fmt.Println(workerName + " leaving Job_" + job.Def.Job_id + "\n")
    wgJob.Done()
}

/* Job processing.
This method shall process the specified receiver job by launching as many goroutines as the number of workers defined in the
specified receiver job.
The job shall be processing until all the workers have ended their work on this job.
*/
func (job *Job) ProcessJob() {
	var wgJob sync.WaitGroup

	// Launching the goroutines workers to work on the job
	nbWorkers := job.Def.NbWorkers
	wgJob.Add(nbWorkers)
	for i := 0; i < nbWorkers; i++ {
		go job.WorkOnJob(i, &wgJob)
	}
	
	// Waiting until the work on job is completed.
	wgJob.Wait()
	fmt.Println("Job_ " + job.Def.Job_id + " completed !")
}


/* Job initialization.
This method shall initialize a job of Job type by:
- assigning the specified receiver JobDef to the Def parameter,
- initializing the urlProcess parameter by creating the UrlProcess for each Job URLs provided by the specified JobDef; indeed for each Job URL:
  the parsed URL of the Job URL, the related waiting URLs, processing URLs and crawled URLs sets.
- creating the Job Status and Result parameters.
Note 1: at this init step, for each Job URL, the waiting URLs set of urlProcess shall contain only the Job URL, with empty associated data.
The processing URLs and crawled URLs shall be empty.
Note 2: at this init step, the InProgress value of the Status parameter shall be initialized to the number of Job URLs.
*/
func (job *Job) InitJob(jobDef *JobDef) {
	// Assigning Def parameter
	job.Def = jobDef

	// Creating ProcessData parameter
	jobProcess := &JobProcess{}
	job.Process = jobProcess

	jobProcess.urlsProcesses = make(map[string]*UrlProcess)
	// For each job URL, initializing the urlProcess for the waitingUrls, processingUrls and crawledUrls sets
	urlsDef := job.Def.Urls
	for _, url := range urlsDef {
 		urlProcess := &UrlProcess{}
 		urlProcess.InitUrlProcess(&url)
    	jobProcess.urlsProcesses[url] = urlProcess
    }

    // Initializing JobStatus and Result parameters of Job
    job.Status = &JobStatus{Completed:0 , InProgress:job.Def.NbWorkers} 
    job.Result = &JobResult{} 
}

/* Getting job status and result end point implementation.
This method shall read the content of an HTTP request, and make sure that the HTTP request is composed by one of the following synthaxis:
1- /jobs/{job_id}/status,
2- /jobs/{job_id}/result.
If the request is one of the cases 1- and 2- above, and if job_id is existing among the allJobs specified receiver parameter, code 200 shall be displayed, and following shall be performed:
- updating the specified job status
- display the response as a new JSON of JobStatus or JobResult type, respectively if request content is "status" or "result".
Else, code 404 shall be caught and displayed. 
*/
func (allJobs *Jobs) GetJobData(w http.ResponseWriter, r *http.Request) {
	urlJobIdPos := 2
	urlServiceNamePos := 3

	// Reading and splitting the URL given in the request
	urlParts := strings.Split(r.URL.String(), "/")

	// Checking if HTTP request is success: code 200 
	if  ( (len(urlParts) == 4) && ((urlParts[urlServiceNamePos] == STATUS)||(urlParts[urlServiceNamePos] == RESULT)) ) {
		// Trying to retrieve the requested job among the jobs datastore
		jobId := urlParts[urlJobIdPos]
		job, existing := allJobs.jobs[jobId]
		if(existing) {
			// Job existing: updating the status of the requested job, necessary to display either status or result data response
			job.Status.Lock()
			job.UpdateJobStatus()
			job.Status.Unlock()

			// Displaying the JSON response according to the request content 
			if  (urlParts[urlServiceNamePos] == STATUS) {
				WriteJson(w, job.Status)
			} else if (urlParts[urlServiceNamePos] == RESULT) {
		   		WriteJson(w, job.Result)
			}
		} else {
			// Job request not exiting: request incorrect: code 404
			w.WriteHeader(http.StatusNotFound)
			return
		}	
	// HTTP request incorrect: code 404
	} else {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

/* Adding job end point implementation.
This method shall read the content of an HTTP request, and make sure that the request is JSON content type.
The JSON decoded shall be a JobDef structure type. 
If the request is not a JSON content type or malformed JSON, code 400 shall be caught and displayed.
Else, code 200 shall be caught and displayed, and following shall be performed:
- create a unique job_id value,
- make sure that there is at least one worker,
- display the response as a new JSON of JobDef type that shall be the same as the request one, with the value to job_id added,
- initialize the new job as Job type with the parameters specified in the request,
- add this new job to the allJobs specified receiver,
- once all the above steps completed, start a goroutine to process the created job.
*/
func (allJobs *Jobs) AddJob(w http.ResponseWriter, r *http.Request) {
	jobDef := &JobDef{}

	// Reading the HTTP request content
	bytes, _ := ioutil.ReadAll(r.Body)

	// Decoding the JSON content: code 400 is incorrect 
	ct := r.Header.Get("content-type")
	err := json.Unmarshal(bytes, jobDef)
	if (err != nil) || (ct != "application/json") {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} 

	// Creating a unique job_id
	jobDef.Job_id = fmt.Sprintf("%d", time.Now().UnixNano())

	// Setting number or workers at least equal to 1
	if(jobDef.NbWorkers < 1){
		jobDef.NbWorkers = 1
	}

	// Displaying the JSON response with job_id defined, and code 200 if success
	WriteJson(w, jobDef)

	// Initializing the new job and adding it to allJobs
	newJob := &Job{}
	newJob.InitJob(jobDef)
	allJobs.jobs[jobDef.Job_id] = newJob

	// Starting the goroutine to process the job
	ready := true
	if(ready){
		go newJob.ProcessJob()	
	}

}


/* Entry point of the API*/
func main() {

	allJobs := Jobs{ jobs : map[string]*Job{} }

	// Adding a Job end point
	http.HandleFunc("/jobs", allJobs.AddJob)
	// Getting a Job status and result end point
	http.HandleFunc("/jobs/", allJobs.GetJobData)

	// Opening URL connection on http://localhost:PORT
    http.ListenAndServe(PORT, nil)

}
