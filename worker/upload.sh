curl -v -X POST http://localhost:808$1/put/ \
  -F "file=@$2" \
  -H "Content-Type: multipart/form-data"
