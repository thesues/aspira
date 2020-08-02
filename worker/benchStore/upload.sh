curl -v -X POST http://localhost:808$1/put/200/ \
  -F "file=@$2" \
  -H "Content-Type: multipart/form-data"
