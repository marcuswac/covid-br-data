import gzip
import os
import requests
import sys

def main(args):
    output_dir = args[0] if len(args) > 0 else "."

    ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
           "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    chunk_size = 1024 * 1024 # 1MB
    base_url = "https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/"

    for uf in ufs:
        i = 1
        read_next = True
        while read_next:
            csv_file = "dados-{}-{}.csv".format(uf.lower(), i)
            url = base_url + csv_file
            response = requests.get(url, stream=True)
            print(f"GET {url}. Status: {response.status_code}")
            
            if response.status_code != 200 and i == 1:
                # state has a single CSV file
                csv_file = "dados-{}.csv".format(uf.lower(), i)
                url = base_url + csv_file
                response = requests.get(url, stream=True)
                print(f"GET {url}. Status: {response.status_code}")
                read_next = False
            
            if response.status_code == 200:
                csv_gz_file = csv_file + '.gz'
                output_file = os.path.join(output_dir, csv_gz_file)
                with gzip.open(output_file, 'wb') as f:
                        for block in response.iter_content(chunk_size):
                            f.write(block)
                print("File saved:", output_file)
            else:
                read_next = False
            i += 1


if __name__ == '__main__':
    main(sys.argv[1:])
