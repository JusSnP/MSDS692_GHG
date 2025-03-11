# Scope 3 Greenhouse Gas Emissions 
### MSDS 692 S8W1 2025 - Regis University  
### 
  
This repository contains notebooks and a python library associated with work performed during MSDS 692 - Data Science Practicum 1 as a member of Dr. Sorauf's Greenhouse Gas Emissions research group. This project's focus was to acquire and understand new features to incorporate into the existing data which will be used by the team to create models *predicitng the amount of scope 3 greenhouse gas emissions*. Additionally XGBoost and feed-forward neural network models were compared as a baseline for future work within the group. For more details please see my [final presentation](/presentation/Parsons_GHG_MSDS692_v8.pptx).

### Contents

* Code - Three notebooks
    * FMP_pull_v1.0 - Jupyter notebook, pulls data using the [Financial Modeling Prep](https://site.financialmodelingprep.com/) (FMP) API. Note the API key was removed from this notebook since it is a licensed key.
    * FMP_API_library - Python source file, library containing API call functions
    * FMP_analysis - EDA and feature engineering of new FMP data and place-name treatment carried over from original research group analysis/
    * mm_nn - Baseline feed-forward neural network used to compare against the groups XGBoost model which was done prior to the work contained in this repo. This neural network *does not* use the new FMP features and is intended as a baseline using an expanded 10-year mega merged dataset vs the group's original 1 year's worth of data.
* Data - CSV files which are timestamped. If importing please take note of file date-time group suffix. Where indicated the files contain new (to this project) data pulled from FMP and descriptions can be obtained from their website. If unspecified, the data is purely CDP and old FMP data.
    * covariance_with_scope3_ - output of covariance calculations all features and constant scope 3 emssion type with scope 3 emission amount
    * EC_data_ - FMP data: Symbol_1, periodOfReport, employeeCount (non-historical data)
    * EC_Hist_data - FMP data: Symbol_1, year, employeeCount (historical 2013-2023)
    * ESG_data - FMP data: Symbol_1, date, environmentalScore, socialScore, governanceScore, ESGScore
    * Mega_merged_all_real_values - Original dataset used by the research group containing multiple categorical and quantitative emissions features plus financial features from original FMP pulls.
    * mm_fmp_data_ - Mega merged file with new FMP features merged
* Presentation - Final presentation materials delivered on 3/6/2025

### Summary
This is a non-comprehensive summary of the work completed during MSDS 692.

The API used to call FMP data was updated since the reserach group's original pull which required some updating. Additionally there were issues with how the data was being sorted, organized, and merged through the API call libraries. There were consistent issues with what is assumed to be server-side timing issues producing inconsistent numbers and composition of data retrieved from FMP.  

![comparison of new and old historical employee count data pulls](/images/EC_discrepancy.png)  

EDA and light feature engineering was conducted on the new data. It was found that the majority of companies represented within the newly acquired features are US corporations, or traded on US stock exchanges.

![company makeup of ESG data](/images/ESG_countries.png)  

Covariances of Scope_3_emission_type with all other features was calculated for each of the 15 unique emissions types contained within our data. It was discovered that, indeed, each emission type had unique covariances and they should be treated individually as they during outlier treatment being conducted by other team members.  

![covariance for downstream leased assets](/images/covariance.png)  
![covariance for downstream transportation and distribution](/images/covariance2.png)  

Finally, the teams original XGBoost model was re-run using the newly acquired 10 year's worth of data *without new FMP data*. This was then compared to a simple feed-forward neural network as a baseline for future work and a determining factor if neural networks could be leveraged for this work.  

![Neural network loss curves](/images/nn.png)  

![XGBoost MAE](/images/XGBoost.png) 




