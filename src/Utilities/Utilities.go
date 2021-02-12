package Utilities

import (
   "encoding/json"
   "net/http"
)

/* Removing duplicates from a slice of string.
This method shall remove the duplicates values in the specified slice of string.
*/
func RemoveSliceDuplicates(slices []string) []string { 
   keys := make(map[string]bool) 
   uniques := []string{} 

   for _, val := range slices {
      if _, valSeen := keys[val]; !valSeen {
         keys[val] = true
         uniques = append(uniques, val) 
      } 
   } 
   return uniques 
} 

/* Responsing a JSON HTTP.
This method shall transform the specified data structure into a JSON content type, and send an HTTP response of it.
If the transformation is failed, code 400 shall be caught and displayed, else code 200 shall be caught and displayed.
*/
func WriteJson(w http.ResponseWriter, data interface{}) { 
   bytes, err := json.Marshal(data)
   // JSON HTTP incorrect: code 400
   if err != nil {
      w.WriteHeader(http.StatusBadRequest)
      w.Write([]byte(err.Error()))
      return
   }
   // JSON HTTP correct: code 200
   w.Header().Add("content-type", "application/json")
   w.WriteHeader(http.StatusOK)
   w.Write(bytes)
} 

