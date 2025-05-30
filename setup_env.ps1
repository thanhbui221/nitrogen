# Read .env file and set environment variables
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        
        # Remove any quotes
        $value = $value -replace '^["'']|["'']$'
        
        # Expand any environment variables in the value
        $value = [System.Environment]::ExpandEnvironmentVariables($value)
        
        Write-Host "Setting $name to $value"
        [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

# Verify the environment variables
Write-Host "`nVerifying environment variables:"
Write-Host "JAVA_HOME: $env:JAVA_HOME"
Write-Host "SPARK_HOME: $env:SPARK_HOME"
Write-Host "HADOOP_HOME: $env:HADOOP_HOME"
Write-Host "PYTHONPATH: $env:PYTHONPATH"
Write-Host "PATH: $env:PATH" 