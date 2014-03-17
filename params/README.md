# Mortar Recommender System parameters

There are a number of parameters that need to be set when running a Mortar recommender.  Sensible defaults can be found in the file params/my-recommender.params but here is a brief explanation of the required parameters.

## Basic Parameters

These parameters are ones that generally depend on your specific job and your preferences.  

* **default_parallel:** Use default\_parallel to improve performance.  This tells Hadoop how many reducers to use.  A good rule of thumb is to set this to be 3 * ([YOUR\_CLUSTER\_SIZE] - 1).
* **NUM_RECS_PER_ITEM:** This is the maximum number of recommendations that will be generated for a single item.
* **NUM_RECS_PER_USER:** This is the maximum number of recommendations that will be generated for a single user.
* **LOGISTIC_PARAM:** Multiple links between a given user and item are allowed and will be aggregated using a logicstic scale which simulates "diminishing returns".  If this parameter is large, the diminishing returns take effect quickly; if it is small, they take effect slowly.  You can use scripts/logistic_scale_vis.py to help tune this parameter.
* **MIN_LINK_WEIGHT:** The raw similarity weight between two items is calculated as the sum, for each user with a link to both of the items, of the weight of the smaller of the two links.  Any link with a weight smaller than this parameter will be dropped.  This can be a large performance improvement if you have a lot of low-weight edges (for example if you have a lot of view data for a website).  This filtering is applied after the logistic scale is applied.
* **BAYESIAN_PRIOR:** This parameter guards the recommender system against the effects of items with small sample sizes (not many users connected to it).  You should set this to be approximately the minimum sample size that you think is sufficient for statistical inference.  One rule of thumb is to set this to the 25% quantile value of the distribution of all item's sample sizes.  For some heavily skewed distributions it is sometimes best to set this as large as the 40% quantile or the median.  You can use the macro recsys\_\_GetQuantiles in macros/util.pig to help find the relevant quantiles in your data.


## Advanced Parameters 

These are parameters that you should rarely need to change.
* **MAX_LINKS_PER_USER:** If any user is connected to more than this number of items, only the top links (by weight) will be kept.  Each user generates O([# links]^2) intermediate results, so an outlier user with many links can have a significant negative performance impact for little improvement on the recommendations generated.
* **ADD_DIVERSITY_FACTOR:** This should be either 'true' or 'false'.  If 'true' the recommendation system will add a 'diversity factor' to try and generate recommendations for a variety of reasons even if those recommendations might not have the highest weight for the user.
