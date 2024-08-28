REM document for documenting the steps I'm taking to get to google bigquery tansformations and stuff

REM 1.0 - setup virtual environment - take from week 1 of data warehousing w/ python 3.11 as base for requirements
C:\Users\MarvinEspinoza-Leiva\AppData\Local\Programs\Python\Python311\python.exe -m venv env_311

REM 2.0 - activate virtual environment
.\env_311\Scripts\Activate.ps1

REM 2.5 - check to see what python version i'm using
python --version

REM 3.0 - create requirements.in file while in following directory: C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc
code requirements.in

REM 3.2 - fill in required packages and versions for google bigquery project
dbt-core==1.5.2
dbt-bigquery==1.5.2
dbt-duckdb==1.5.2
google-cloud-bigquery==3.5.0
google-cloud-storage==2.5.0
google-auth-oauthlib>=0.5.2,<0.6.0
google-cloud-bigquery-storage

REM 3.3 - install and use pip-tools
pip install pip-tools
pip-compile requirements.in

REM 3.5 - install the needed packages within the requirements folder
py -m pip install -r requirements.txt

REM 4.0 - now we need to download and install google sdk cli

REM 5.0 - go through steps to get authenticated using credentials via web browser
REM 5.1 - verify google cloud sdk installation: C:\Users\[YourUsername]\AppData\Local\Google\Cloud SDK
REM 5.2 - add google cloud sdk to PATH: 
$env:PATH += ";C:\Users\MarvinEspinoza-Leiva\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin"

REM 5.3 - set up authentication scopes
$scopes = "https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/iam.test"

REM 5.4 - run authentication command
gcloud auth application-default login --scopes=$scopes

REM 5.5 - faster version of 5.3-5.4: gcloud auth application-default login --scopes="https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/iam.test"

REM 5.6 - verify authentication
gcloud auth list

REM 5.7 - NOTE: For scripts that use these credentials, you may need to set the GOOGLE_APPLICATION_CREDENTIALS environment variable:
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\MarvinEspinoza-Leiva\AppData\Roaming\gcloud\application_default_credentials.json"

REM 5.8 - Other IMPORTANT NOTES: Important Notes
    REM The authentication process creates a file named application_default_credentials.json in your user's AppData folder. Keep this file secure.
    REM The authenticated scopes include cloud-platform (required), bigquery, drive.readonly, and iam.test.
    REM If you're using a specific project, you might need to set it with gcloud config set project [YOUR_PROJECT_ID].

REM 5.9 - when you close and re-open PowersShell: (# = comments in powershell)
    REM # gcloud-setup.ps1

    REM # Add Google Cloud SDK to PATH
    REM $env:PATH += ";C:\Users\MarvinEspinoza-Leiva\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin"

    REM # Set GOOGLE_APPLICATION_CREDENTIALS environment variable
    REM $env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\MarvinEspinoza-Leiva\AppData\Roaming\gcloud\application_default_credentials.json"

    REM # Verify gcloud is accessible
    REM gcloud version

    REM # Check authentication status
    REM gcloud auth list

    REM # If you need to reauthenticate, uncomment and run the following line:
    REM # $scopes = "https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/iam.test"
    REM # gcloud auth application-default login --scopes=$scopes

    REM Write-Host "Google Cloud SDK environment setup complete."

REM 5.9.1 - Note on reauthenticating -- 
    REM You can save this script as gcloud-setup.ps1 in a convenient location. 
    REM Then, whenever you open a new PowerShell session and want to work with Google Cloud SDK, you can run:
. C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc\gcloud-setup.ps1


REM 6.0 - create bigquery project variable name

$env:BIGQUERY_PROJECT="gsnc-datawarehouse-v1"



