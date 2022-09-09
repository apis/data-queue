$nats_hub = Start-Process -PassThru -FilePath ".\tools\nats\nats-server.exe" -ArgumentList "-c hub.conf" -WorkingDirectory ".\tools\nats"
$nats_leaf = Start-Process -PassThru -FilePath ".\tools\nats\nats-server.exe" -ArgumentList "-c leaf.conf" -WorkingDirectory ".\tools\nats"

$server = Start-Process -PassThru -FilePath ".\server.exe"
Start-Sleep -Seconds 1
#$producer = Start-Process -PassThru -FilePath ".\producer.exe" -ArgumentList "--bucket bucket2 --natsName producer2"
#$consumer = Start-Process -PassThru -FilePath ".\consumer.exe" -ArgumentList "--bucket bucket2 --natsName consumer2"

echo "Press any key to stop ..."
[Console]::ReadKey()

#Stop-Process -InputObject $consumer
#Stop-Process -InputObject $producer
Stop-Process -InputObject $server

Stop-Process -InputObject $nats_leaf
Stop-Process -InputObject $nats_hub
