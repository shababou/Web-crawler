package UrlCrawling

import (
   "fmt"
   "golang.org/x/net/html"
   "net/http"
   "net/url"
   "strings"
   "sync"
)

type MapUrls struct {
   sync.Mutex
   Urls map[string][]string
}

type UrlData struct {
   sync.Mutex
   DomainUrl *url.URL
   ChildrenUrls map[string]string
   Data []string
}



// Helper function to pull the href attribute from a Token
func getTokenValue(token html.Token, tag string) (ok bool, val string) {
   // Iterate over token attributes until we find an "href"
   for _, att := range token.Attr {
      if att.Key == tag {
         val = att.Val
         ok = true
      }
   }
   
   // "bare" return will return the variables (ok, href) as 
    // defined in the function definition
   return
}

// Extract all http** links from a given webpage
func (urlData *UrlData) CrawlUrl(urlToCrawl *string, crawledUrls *MapUrls, crawled chan bool) { 
   urlContent, err := http.Get(*urlToCrawl)
   if err != nil {
      fmt.Println("ERROR: Failed to crawl:", *urlToCrawl)
      return
   }
   urlBody := urlContent.Body
   defer urlBody.Close() // close Body when the function completes

   domainUrl := urlData.DomainUrl

    //  fmt.Println("CRAWL")
   //fmt.Println(*urlToCrawl)

   collectHref := false
   urlExtension := strings.SplitAfter(*urlToCrawl, domainUrl.String())
   if( (*urlToCrawl == domainUrl.String()) || (len(strings.Split(urlExtension[len(urlExtension)-1], "/")) <= 1) ) {
      collectHref = true
   }
   urlTokenizer := html.NewTokenizer(urlBody)
   for {
      tokenizeItem := urlTokenizer.Next()
      switch {
         case tokenizeItem == html.ErrorToken:
            //fmt.Println("return " + *urlToCrawl)
              // fmt.Println("ENDING CRAWL")
               crawled <- true

            return
         case tokenizeItem == html.StartTagToken:
            token := urlTokenizer.Token() 
            if((token.Data == "a")||(token.Data == "link")) {
               // Check if the token is an <a> tag
               if collectHref { 
                  // Extract the href value, if there is one
                  hasLink, link := getTokenValue(token, "href")
                  if hasLink {
                     parsedUrl, errParse := ParseUrl(&link)
                     if(errParse==nil){
                        linkAbs := domainUrl.ResolveReference(parsedUrl)
                        crawledUrls.Lock()
                        if _, ok := crawledUrls.Urls[linkAbs.String()]; !ok{
                           //linkAbsSplit := strings.Split(linkAbs.String(), ".")
                           if((linkAbs.Host == domainUrl.Host)){ //&& (linkAbsSplit[len(linkAbsSplit)-1] == "html")){  
                              urlData.Lock()
                              urlData.ChildrenUrls[linkAbs.String()] = "" 
                              urlData.Unlock()     
                           }  
                        }
                        crawledUrls.Unlock()
                     }
                  }    
               }
            } else if (token.Data == "img") {
               // Extract the href value, if there is one
               hasImg, img := getTokenValue(token, "src")
               if hasImg {
                  parsedUrl, errParse := ParseUrl(&img)
                  if(errParse==nil){
                     imgAbs := domainUrl.ResolveReference(parsedUrl)
                     imgAbsSplit := strings.Split(imgAbs.String(), ".")
                     imgAbsExtension := imgAbsSplit[len(imgAbsSplit)-1] 
                     if((imgAbsExtension == "png") || (imgAbsExtension == "gif") || (imgAbsExtension == "jpeg")){
                        urlData.Lock()
                        urlData.Data = append(urlData.Data, imgAbs.String())      
                        urlData.Unlock()          
                     }
                  }
               }
            }
      } 

   }

}

/* URL parsing.
This method shall parse the specified URL and return the parsed URL with the associated error.
*/
func ParseUrl(urlToParse *string) (*url.URL, error) {
   urlParsed, err := url.Parse(*urlToParse)

   return urlParsed, err
}

/* UrlData initialization.
This method shall initialize a UrlData by:
- assigning the parsed specified URL to the domainUrl parameter,
- initializing the childrenUrls parameter with the specified URL only,
- initializing the data parameter to empty.
*/
func (urlData *UrlData) InitUrlData(urlToParse *string) {
   // Assigning the parsed specified URL
   parsedUrl, _ := ParseUrl(urlToParse)
   urlData.DomainUrl = parsedUrl
   // Initializing the childrenUrls parameter
   urlData.ChildrenUrls = map[string]string{*urlToParse:""}
   // Initializing the data parameter
   urlData.Data = []string{}
}

