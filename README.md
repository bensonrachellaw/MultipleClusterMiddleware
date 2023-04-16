# MultipleClusterMiddleware
MultipleClusterMiddleware Project based on the existing RSP(Random Sample Partition) big data technology.

# Cross data-centre collaborative parallel computing architecture and MultipleClusterMiddleware：

The computing architecture supports efficient collaborative analysis of big data in multiple geographically isolated data centres. 

Based on the existing RSP big data technology, the big data from each data centre is expressed as an RSP data model, a portion of data block samples are randomly selected to do big data analysis within the cluster using the same algorithm, and then the results from all data centres are transmitted to the central console for integration to obtain the analysis results of cross-data centre big data. 

If the data distribution of each cluster subset is inconsistent, cross-domain merging of sample blocks from different clusters is used, and the distribution of the merged data blocks is consistent with the distribution of cross-cluster big data, and the above analysis is done on the merged data blocks to obtain the analysis results of cross-cluster big data.
