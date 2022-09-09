$nats_hub = Start-Process -PassThru -FilePath ".\tools\nats\nats-server.exe" -ArgumentList "-c hub.conf -l hub.log" -WorkingDirectory ".\tools\nats"
$nats_leaf = Start-Process -PassThru -FilePath ".\tools\nats\nats-server.exe" -ArgumentList "-c leaf.conf -l leaf.log" -WorkingDirectory ".\tools\nats"

$server = Start-Process -PassThru -FilePath ".\server.exe"
Start-Sleep -Seconds 1
#$client = Start-Process -PassThru -FilePath "..\client\client.exe" -WorkingDirectory "..\client"
$producer = Start-Process -PassThru -FilePath ".\producer.exe"

echo "Press any key to stop ..."
[Console]::ReadKey()

Stop-Process -InputObject $producer
Stop-Process -InputObject $client
Stop-Process -InputObject $server

Stop-Process -InputObject $nats_leaf
Stop-Process -InputObject $nats_hub
