import os
import random

def generate_random_sequence(length):
    return ''.join(random.choice('ACGT') for _ in range(length))

def generate_fastq_entry(seq_id, sequence, quality):
    return f"@{seq_id}\n{sequence}\n+\n{quality}\n"

def create_large_fastq_file(file_name, target_size_mb):
    target_size_bytes = target_size_mb * 1024 * 1024
    current_size = 0
    seq_id = 1
    sequence_length = 100  # Length of each DNA sequence
    quality_score = 'I' * sequence_length  # High-quality score

    with open(file_name, 'w') as f:
        while current_size < target_size_bytes:
            sequence = generate_random_sequence(sequence_length)
            fastq_entry = generate_fastq_entry(seq_id, sequence, quality_score)
            f.write(fastq_entry)
            current_size += len(fastq_entry.encode('utf-8'))
            seq_id += 1

if __name__ == "__main__":
    create_large_fastq_file("westn/raw/abc1120_agrf_wes.fastq", 14)
    create_large_fastq_file("westn/raw/abc1121_agrf_wes.fastq", 11)
    print("FASTQ file has been created.")

