cat $(ls | grep -E "^train_data_[0-9]+_${1}\.csv$") >  "train_data_${1}.csv"
sed -E -e 's/nan/0/g' "train_data_${1}.csv" -i
