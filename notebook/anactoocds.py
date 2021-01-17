import flattentool
import json
import csv
from anacocds_work import *



csv.QUOTE_NONE


data_dir ='../csv/2020/'
tmp_dir = '../csv/tmp/'
schema_dir = '../csv/'
out_dir = '../data/'

date = '2020-10-15T00:00:00Z'

def main(cod_pa):

    work_dir = data_dir + cod_pa

    agg_input = read_aggiudicatari(work_dir)

    par_input = read_partecipanti(work_dir)

    lot_input = read_lotti(work_dir)

    releases = create_releases(lot_input, date)

    parties = create_parties(par_input, agg_input, lot_input, date)

    tende_tenderers = create_tende_tenderers(par_input, date)

    awards = create_awards(lot_input, date)

    awards_suppliers = create_awards_suppliers(agg_input, date)

    contr_imple_transactions = create_contr_imple_transactions(lot_input, date)

    contracts = create_contracts(agg_input, date)

    write_out = os.path.join(tmp_dir, "releases.csv")
    releases.to_csv (write_out, index = None, header=True) 


    write_out = os.path.join(tmp_dir, "parties.csv")
    parties.to_csv (write_out, index = None, header=True) 


    write_out = os.path.join(tmp_dir, "tende_tenderers.csv")
    tende_tenderers.to_csv (write_out, index = None, header=True) 


    write_out = os.path.join(tmp_dir, "awards.csv")
    awards.to_csv (write_out, index = None, header=True) 


    write_out = os.path.join(tmp_dir, "awards_suppliers.csv")
    awards_suppliers.to_csv (write_out, index = None, header=True) 


    write_out = os.path.join(tmp_dir, "contr_imple_transactions.csv")
    contr_imple_transactions.to_csv (write_out, index = None, header=True) 

    write_out = os.path.join(tmp_dir, "contracts.csv")
    contracts.to_csv (write_out, index = None, header=True) 

    filename = 'ocds.json'
    filename_dir = out_dir + 'IT-CF-' + cod_pa + '/'
    out_path = os.path.join(filename_dir, filename)


    if not os.path.exists(filename_dir):
        os.makedirs(filename_dir)


    schema_path = os.path.join(schema_dir, 'release-schema.json')
    base_path = os.path.join(work_dir, 'base.json')

    flattentool.unflatten(tmp_dir, 
                            output_name = out_path,
                            input_format='csv',
                            root_id='ocid',
                            base_json = base_path, 
                            root_list_path='releases',
                            schema = schema_path
                            
                        
                            )


if __name__ == "__main__":


    for name in os.listdir(data_dir):
        try:
            print('Processing ', name)
            main(name)
        except:
            print('errore su ', name)

