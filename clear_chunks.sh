#!/bin/bash

# Clear all chunk files
echo "Removing chunk files..."
rm -f chunk_*.txt

# Count remaining chunk files
count=$(ls chunk_*.txt 2>/dev/null | wc -l)
if [ $count -eq 0 ]; then
    echo "All chunk files removed successfully!"
else
    echo "$count chunk files still remain"
fi
