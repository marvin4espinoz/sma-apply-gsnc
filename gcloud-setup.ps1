# gcloud-setup.ps1

# Add Google Cloud SDK to PATH
$env:PATH += ";C:\Users\MarvinEspinoza-Leiva\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin"

# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\MarvinEspinoza-Leiva\AppData\Roaming\gcloud\application_default_credentials.json"

# Verify gcloud is accessible
gcloud version

# Check authentication status
gcloud auth list

# If you need to reauthenticate, uncomment and run the following line:
$scopes = "https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive.readonly,https://www.googleapis.com/auth/iam.test"
gcloud auth application-default login --scopes=$scopes

Write-Host "Google Cloud SDK environment setup complete."

# to run this, type into powershell terminal:
# . C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc\gcloud-setup.ps1