# Get the total number of lines in the file
# total_lines=$(wc -l < measurements.txt)
total_lines=1000000000

echo $total_lines
# Calculate a tenth of the total lines
quarter_lines=$((total_lines / 10))

# Read only the first quarter of the file
head -n $quarter_lines measurements.txt > sample.txt