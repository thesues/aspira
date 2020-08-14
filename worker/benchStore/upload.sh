curl -v -X POST http://localhost:808$1/put/$2/ \
  -F "file=@$3" \
  -H "Content-Type: multipart/form-data"
