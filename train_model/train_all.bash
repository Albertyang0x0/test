datas=$(ls ../binary_train_data/*)
for data in ${datas}; do
  lightgbm config=train.conf data=${data} output_model=$(echo ${data} | sed -E -e 's/^\.\.\/binary_train_data\///')
done
