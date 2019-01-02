## What is this?
Read README-old.md for more information

## Deployment 
Azure is 2-3 years behind so pipenv is not supported yet https://github.com/MicrosoftDocs/azure-docs/issues/11044
-> old school pip should works fine

### Preparation
- Install python3.6.4 64 bits (azure has install extension feature, should be easy): https://blogs.msdn.microsoft.com/azureossds/2016/12/09/running-python-webjob-on-azure-app-services-using-non-default-python-version/
- Cross check python/pip paths 
    - Go to kudu console, find your python installation, should be in D:\home\..
    - Modify our run.cmd if they don't match (run.cmd uses specified python/pip to install and run the project)

### Deployment
- Modify your db connection string in bitfinex/db.py
- Zip folder and add to web jobs


