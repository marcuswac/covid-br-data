import gzip
import os
import requests
import sys

def main(args):
    output_dir = args[0] if len(args) > 0 else "esus-notifica"

    ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
        "PB", "PE", "PI", "PR", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

    for uf in ufs:
        csv_file = "dados-{}.csv".format(uf.lower())
        csv_gz_file = csv_file + '.gz'
        output_file = os.path.join(output_dir, csv_gz_file)
        url = "https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/" + csv_file
        
        response = requests.get(url, stream=True)

        with gzip.open(output_file, 'wb') as f:
            for block in response.iter_content(1024):
                f.write(block)
        
        print("File saved:", output_file)

if __name__ == '__main__':
    main(sys.argv[1:])
