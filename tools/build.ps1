go build -v .\cmd\producer
copy .\cmd\producer\producer_config .

go build -v .\cmd\server
copy .\cmd\server\server_config .

go build -v .\cmd\consumer
copy .\cmd\consumer\consumer_config .
