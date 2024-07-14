import os
import random

def generate_header():
    header = ["GeneID"]
    for i in range(1111, 1123):
        header.append(f"abc{i}")
    return "\t".join(header) + "\n"

def generate_gene_line(gene_name):
    values = [gene_name]
    values.extend(str(random.randint(0, 10)) for _ in range(12))
    return "\t".join(values) + "\n"

def create_large_file(file_name, target_size_gb):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    current_size = 0

    with open(file_name, 'w') as f:
        header = generate_header()
        f.write(header)
        current_size += len(header.encode('utf-8'))

        genes = ["GeneA", "GeneB", "GeneC", "GeneD", "GeneE"]

        while current_size < target_size_bytes:
            for gene in genes:
                gene_line = generate_gene_line(gene)
                f.write(gene_line)
                current_size += len(gene_line.encode('utf-8'))
                if current_size >= target_size_bytes:
                    break

if __name__ == "__main__":
    create_large_file("new_big_all.counts.tsv", 2)
    print("2GB file 'new_big_all.counts.tsv' has been created.")

