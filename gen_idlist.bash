
if [ $# -ne 1 ]; then
  echo "Usage: $0 <datapack>"
  exit 1
fi

ids=()

for id in $(ls ../$1/log/); do
  ids[${id:0:36}]=1
done

for id in $(ls ../$1/trace/); do
  ids[${id:0:36}]=1
done

id_array=()

for id in ${!ids[@]}; do
  id_array+=${id}
done

len=${#id_array[@]}
count=0
for (( i=0; i<len; i+=350 )) do
  max=i
  if [ ${i} -gt ${len} ]; then
    max=${len}
  fi
  filename="idlist_"${count}"_"$1".txt"
  for (( j=i; j<max; j++ )) do
    echo ${id_array[j]}
  done > filename
done

exit 0
