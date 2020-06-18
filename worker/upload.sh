curl -v -X POST http://localhost:8081/put/ \
  -F "file=@$1" \
  -H "Content-Type: multipart/form-data"
