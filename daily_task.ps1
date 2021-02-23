<# powershell task scheduling command (from Shane Young - https://www.youtube.com/watch?v=izlIJTmUW0o)
    
    $action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument "-executionpolicy bypass -noprofile -file 'D:\Workspace\Pittsburgh Police Blotter\daily_task.ps1"
    $trigger = New-ScheduledTaskTrigger -Daily -At 12pm
    Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "PittPoliceBlotterPull" -Description "Daily Blotter Pull"

#>

$yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
$wshell = New-Object -ComObject Wscript.Shell
$result = python D:\Workspace\"Pittsburgh Police Blotter"/record_pull.py $yesterday "Automated" # may need to change depending on where yours is stored

$Output = $wshell.Popup($result)