#!/bin/bash

input="$1"
period=$(echo "1.0/$4" | bc -l)

while IFS= read -r var
do
  sleep $period
  redis-cli -h $2 publish "$3" "$var"
done < "$input"
