package Utilities

import (
   //"fmt"
   "encoding/json"
   "net/http"
)
/*
func MergeSlices(slices [][]string) []string {
   var mergedSlice []string
   for _, slice := range slices {
      fmt.Printf("2**%d = %d\n", i, v)
      mergedSlice = append(mergedSlice, slice)
   }
   return mergedSlice
}*/

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


func WriteJson(w http.ResponseWriter, data interface{}) { 
   bytes, err := json.Marshal(data)
   if err != nil {
      w.WriteHeader(http.StatusBadRequest)
      w.Write([]byte(err.Error()))
      return
   }

   w.Header().Add("content-type", "application/json")
   w.WriteHeader(http.StatusOK)
   w.Write(bytes)
} 

