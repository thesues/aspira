n=0
while [[ $n -le 50 ]]; do
	sh upload.sh $1 $2 ~/upstream/fuli/xikali.png
	((n++))
done
