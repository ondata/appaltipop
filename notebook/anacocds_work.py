import os
import pandas as pd
import numpy as np


ocid_code = 'bxokds'





def read_aggiudicatari (work_dir):

    agg_csv_path = os.path.join(work_dir, "aggiudicatari.csv")
    agg_input = pd.read_csv(agg_csv_path,  dtype={'codiceFiscale':'object'}, encoding='utf-8')
    
    #Elimina righe con cig NULL
    agg_input = agg_input[agg_input['cig'].notna()]

    #Assegna valore univoco agli aggiudicatari con cig = 0000000000
    agg_input['cig'] = np.where(agg_input['cig'] == '0000000000', agg_input['lottoID'], agg_input['cig'])
    agg_input['cig'] = np.where(agg_input['cig'] == '00000000000', agg_input['lottoID'], agg_input['cig'])

    #Elimina righe con cig 0

    agg_input = agg_input[agg_input['cig']!='0']

    #Trasforma in carattere maiuscolo i cig
    agg_input['cig'] = agg_input['cig'].str.upper()


    #Trasformazione codice fiscale
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].astype(str)
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].str.upper()
    agg_input['codiceFiscale'] = 'IT-CF-' + agg_input['codiceFiscale']

    #Trasformazione codice fiscale estero e assegnazione al field codiceFiscale
    agg_input['identificativoFiscaleEstero'] = agg_input['identificativoFiscaleEstero'].astype(str)
    agg_input['identificativoFiscaleEstero'] = agg_input['identificativoFiscaleEstero'].str.replace('-', '')
    agg_input['identificativoFiscaleEstero'] = agg_input['identificativoFiscaleEstero'].str.upper()
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].fillna('EE-CF-' + agg_input['identificativoFiscaleEstero'])


    #Creazione codice OCID
    agg_input['ocid'] = 'ocds-' + ocid_code + '-' + agg_input['cig']

    #cancella righe con codiceFiscale NaN
    #agg_input = agg_input[agg_input['codiceFiscale'].notna()]

    #Eliminazione caratteri indesiderati
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].str.replace(' ', '')
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].str.replace('.', '')
    agg_input['codiceFiscale'] = agg_input['codiceFiscale'].str.replace(':', '')

    agg_input = agg_input.drop_duplicates(subset=['cig', 'codiceFiscale'])
    agg_input.reset_index(drop=True)

    return agg_input


def read_partecipanti (work_dir):

    par_csv_path = os.path.join(work_dir, "partecipanti.csv")
    par_input = pd.read_csv(par_csv_path, dtype={'codiceFiscale':'object'}, encoding = 'utf-8')
    
    

    #Elimina righe con cig NULL
    par_input = par_input[par_input ['cig'].notna()]

    #Assegna valore univoco agli aggiudicatari con cig = 0000000000
    par_input['cig'] = np.where(par_input['cig'] == '0000000000', par_input['lottoID'], par_input['cig'])
    par_input['cig'] = np.where(par_input['cig'] == '00000000000', par_input['lottoID'], par_input['cig'])

    #Elimina righe con cig 0
    par_input = par_input[par_input['cig']!='0']

    #Trasforma in carattere maiuscolo i cig
    par_input['cig'] = par_input['cig'].str.upper()

    #Trasformazione codice fiscale
    par_input['codiceFiscale'] = par_input['codiceFiscale'].astype(str)
    par_input['codiceFiscale'] = par_input['codiceFiscale'].str.upper()
    par_input['codiceFiscale'] = 'IT-CF-' + par_input['codiceFiscale']

    #Trasformazione codice fiscale estero e assegnazione al field codiceFiscale
    par_input['identificativoFiscaleEstero'] = par_input['identificativoFiscaleEstero'].astype(str)
    par_input['identificativoFiscaleEstero'] = par_input['identificativoFiscaleEstero'].str.replace('-', '')
    par_input['identificativoFiscaleEstero'] = par_input['identificativoFiscaleEstero'].str.upper()
    par_input['codiceFiscale'] = par_input['codiceFiscale'].fillna('EE-CF-' + par_input['identificativoFiscaleEstero'])

    #Creazione codice OCID
    par_input['ocid'] = 'ocds-' + ocid_code + '-' + par_input['cig']


    #Eliminazione caratteri indesiderati
    par_input['codiceFiscale'] = par_input['codiceFiscale'].str.replace(' ', '')
    par_input['codiceFiscale'] = par_input['codiceFiscale'].str.replace('.', '')
    par_input['codiceFiscale'] = par_input['codiceFiscale'].str.replace(':', '')

    par_input = par_input.drop_duplicates(subset=['cig', 'codiceFiscale'])

    par_input.reset_index(drop=True)


    return par_input



def read_lotti (work_dir):

    lot_csv_path = os.path.join(work_dir, "lotti.csv")
    lot_input = pd.read_csv(lot_csv_path, dtype={'strutturaProponente:codiceFiscaleProp': object}, encoding = 'utf-8')


    lot_input  = lot_input[lot_input['cig'].notna()]

    #Assegna valore univoco i lotti con cig = 0000000000
    lot_input['cig'] = np.where(lot_input['cig'] == '0000000000', lot_input['lottoID'], lot_input['cig'])

    #cancellazione lotti con cig = 0
    lot_input = lot_input[lot_input['cig']!='0']

    #Trasforma in carattere maiuscolo i cig
    lot_input['cig'] = lot_input['cig'].str.upper()

    #Creazione codice OCID
    lot_input['ocid'] = 'ocds-' + ocid_code + '-' + lot_input['cig']

    #Trasformazione codiceFiscaleProp
    lot_input['strutturaProponente:codiceFiscaleProp'] = lot_input['strutturaProponente:codiceFiscaleProp'].str.upper()
    lot_input['strutturaProponente:codiceFiscaleProp'] = 'IT-CF-' + lot_input['strutturaProponente:codiceFiscaleProp']

    #Trasformazione dataInizio
    lot_input['tempiCompletamento:dataInizio'] = lot_input['tempiCompletamento:dataInizio'] + 'T09:30:00Z'

    #Trasformazione dataUltimazione
    lot_input['tempiCompletamento:dataUltimazione'] = lot_input['tempiCompletamento:dataUltimazione'] + 'T09:30:00Z'

    lot_input.loc[lot_input['sceltaContraente'] == '23-AFFIDAMENTO IN ECONOMIA - AFFIDAMENTO DIRETTO', 'sceltaContraente'] = '23-AFFIDAMENTO DIRETTO'
    lot_input.loc[lot_input['sceltaContraente'] == 'AFFIDAMENTO DIRETTO', 'sceltaContraente'] = '23-AFFIDAMENTO DIRETTO'
    lot_input.loc[lot_input['sceltaContraente'] == 'PROCEDURA NEGOZIATA SENZA PREVIA PUBBLICAZIONE DEL BANDO', 'sceltaContraente'] = '04-PROCEDURA NEGOZIATA SENZA PREVIA PUBBLICAZIONE DEL BANDO'
    lot_input.loc[lot_input['sceltaContraente'] == '04-PROCEDURA NEGOZIATA SENZA PREVIA PUBBLICAZIONE', 'sceltaContraente'] = '04-PROCEDURA NEGOZIATA SENZA PREVIA PUBBLICAZIONE DEL BANDO'
    lot_input.loc[lot_input['sceltaContraente'] == 'AFFIDAMENTO DIRETTO IN ADESIONE AD ACCORDO QUADRO/CONVENZIONE', 'sceltaContraente'] = '26-AFFIDAMENTO DIRETTO IN ADESIONE AD ACCORDO QUADRO/CONVENZIONE'
    lot_input.loc[lot_input['sceltaContraente'] == '22-PROCEDURA NEGOZIATA DERIVANTE DA AVVISI CON CUI SI INDICE LA GARA', 'sceltaContraente'] = '22-PROCEDURA NEGOZIATA CON PREVIA INDIZIONE DI GARA (SETTORI SPECIALI)'
    lot_input.loc[lot_input['sceltaContraente'] == '17-AFFIDAMENTO DIRETTO EX ART. 5 DELLA LEGGE N.381/91', 'sceltaContraente'] = '17-AFFIDAMENTO DIRETTO EX ART. 5 DELLA LEGGE 381/91'
    lot_input.loc[lot_input['sceltaContraente'] == 'PROCEDURA APERTA', 'sceltaContraente'] = '01-PROCEDURA APERTA'
    lot_input.loc[lot_input['sceltaContraente'] == '03-PROCEDURA NEGOZIATA PREVIA PUBBLICAZIONE', 'sceltaContraente'] = '03-PROCEDURA NEGOZIATA PREVIA PUBBLICAZIONE DEL BANDO'
    lot_input.loc[lot_input['sceltaContraente'] == 'PROCEDURA NEGOZIATA PER AFFIDAMENTI SOTTO SOGLIA', 'sceltaContraente'] = '33-PROCEDURA NEGOZIATA PER AFFIDAMENTI SOTTO SOGLIA'
    lot_input.loc[lot_input['sceltaContraente'] == '06-PROCEDURA NEGOZIATA SENZA PREVIA INDIZIONE DI  GARA ART. 221 D.LGS. 163/2006', 'sceltaContraente'] = '06-PROCEDURA NEGOZIATA SENZA PREVIA INDIZIONE DI GARA (SETTORI SPECIALI)'
    lot_input.loc[lot_input['sceltaContraente'] == 'PROCEDURA RISTRETTA', 'sceltaContraente'] = '02-PROCEDURA RISTRETTA'
    lot_input.loc[lot_input['sceltaContraente'] == 'AFFIDAMENTO DIRETTO A SOCIETA\' IN HOUSE', 'sceltaContraente'] = '24-AFFIDAMENTO DIRETTO A SOCIETA\' IN HOUSE'
    lot_input.loc[lot_input['sceltaContraente'] == 'CONFRONTO COMPETITIVO IN ADESIONE AD ACCORDO QUADRO/CONVENZIONE', 'sceltaContraente'] = '27-CONFRONTO COMPETITIVO IN ADESIONE AD ACCORDO QUADRO/CONVENZIONE'
    lot_input.loc[lot_input['sceltaContraente'] == 'PROCEDURA NEGOZIATA SENZA PREVIA INDIZIONE DI GARA (SETTORI SPECIALI)', 'sceltaContraente'] = '06-PROCEDURA NEGOZIATA SENZA PREVIA INDIZIONE DI GARA (SETTORI SPECIALI)'

    #lot_input.reset_index(drop=True)


    return lot_input


def create_releases (lot_input, date, tag, initiationType):

    releases_data = {
                'ocid': lot_input['ocid'],
                'id': lot_input['cig'].values,
                'buyer/name': lot_input['strutturaProponente:denominazione'].values,
                'buyer/id': lot_input['strutturaProponente:codiceFiscaleProp'].values,
                'tender/id': lot_input['cig'].values,
                'tender/title': lot_input['oggetto'].values,
                'tender/procuringEntity/id': lot_input['strutturaProponente:codiceFiscaleProp'].values,
                'tender/procurementMethodDetails': lot_input['sceltaContraente'].values,
                'tender/contractPeriod/startDate': lot_input['tempiCompletamento:dataInizio'].values,
                'tender/contractPeriod/endDate': lot_input['tempiCompletamento:dataUltimazione'].values,    
    
            }


    releases = pd.DataFrame(releases_data, columns = ['ocid', 
                                                    'id', 
                                                    'date', 
                                                    'tag', 
                                                    'initiationType', 
                                                    'buyer/name', 
                                                    'buyer/id', 
                                                    'tender/id', 
                                                    'tender/title', 
                                                    'tender/procuringEntity/name', 
                                                    'tender/procuringEntity/id', 
                                                    'tender/items/id', 
                                                    'tender/procurementMethodDetails',
                                                    'tender/contractPeriod/startDate',
                                                    'tender/contractPeriod/endDate'])


    releases['date'] = date
    releases['tag'] = tag
    releases['initiationType'] = initiationType


    return releases


def create_parties (par_input, agg_input):

    parties_tmp = par_input[['codiceFiscale', 'ragioneSociale', 'ocid', 'cig']].copy()
    agg_tmp = agg_input[['codiceFiscale', 'ragioneSociale', 'ocid', 'cig']].copy()
   
    parties_tmp = parties_tmp.drop_duplicates(subset=['cig', 'codiceFiscale'])
    agg_tmp = agg_tmp.drop_duplicates(subset=['cig', 'codiceFiscale'])


    common = pd.merge(agg_tmp, parties_tmp, on=['cig', 'codiceFiscale'], how='inner')
    common = common.drop_duplicates(subset=['cig', 'codiceFiscale'])
    common_renamed = common.rename(columns={'ragioneSociale_x': 'ragioneSociale', 'ocid_x': 'ocid'})
    common_final = common_renamed[['codiceFiscale', 'ragioneSociale', 'ocid', 'cig']].copy()
    common_final['ruolo'] ='supplier;tenderer'

    parties_only = parties_tmp.merge(common, on=['cig', 'codiceFiscale'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
    agg_only = agg_tmp.merge(common, on=['cig', 'codiceFiscale'], how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']

    parties_final = parties_only[['codiceFiscale', 'ragioneSociale', 'ocid', 'cig']].copy()
    agg_final = agg_only[['codiceFiscale', 'ragioneSociale', 'ocid', 'cig']].copy()

    agg_final['ruolo'] ='supplier'
    parties_final['ruolo'] ='tenderer'

    parties_agg = parties_final.append(agg_final)
    parties_final = parties_agg.append(common_final)

    parties_data = {
                'ocid': parties_final['ocid'].values,
                'id': parties_final['cig'].values,
                'parties/0/name': parties_final['ragioneSociale'].values,
                'parties/0/id': parties_final['codiceFiscale'].values,
                'parties/0/roles': parties_final['ruolo'].values
            }

    parties = pd.DataFrame(parties_data, columns = ['ocid', 
                                                    'id', 
                                                    'parties/0/name', 
                                                    'parties/0/id',
                                                    'parties/0/roles',
                                                    'parties/0/memberOf/id',
                                                    'parties/0/memberOf/name'])


    parties = parties.reset_index().drop_duplicates(subset=['id','parties/0/id'], keep='first').set_index('index')



    return parties


def create_tende_tenderers (par_input):

    tende_tenderers_data = {
             'ocid': par_input['ocid'].values,
              'id': par_input['cig'].values,
             'tender/id': par_input['cig'].values,
             'tender/tenderers/0/name': par_input['ragioneSociale'].values,
             'tender/tenderers/0/id': par_input['codiceFiscale'].values
             
        }

    tende_tenderers = pd.DataFrame(tende_tenderers_data, columns = ['ocid', 
                                                    'id', 
                                                    'tender/id',
                                                    'tender/tenderers/0/name', 
                                                    'tender/tenderers/0/id'])

    return tende_tenderers




def create_awards (lot_input):

    awards_data = {
                'ocid': lot_input['ocid'].values,
                'id': lot_input['cig'].values,
                'awards/0/id': lot_input['cig'].values,
                'awards/0/value/amount': lot_input['importoAggiudicazione'].values

                
            }

    awards = pd.DataFrame(awards_data, columns = ['ocid', 
                                                    'id', 
                                                    'awards/0/id',
                                                    'awards/0/value/amount' 
                                                    ])


    return awards



def create_awards_suppliers (agg_input):

    awards_suppliers_data = {
                          
             'ocid': agg_input['ocid'].values,
             'id': agg_input['cig'].values,
             'awards/0/suppliers/0/name': agg_input['ragioneSociale'].values,
             'awards/0/id': agg_input['cig'].values,
             'awards/0/suppliers/0/id': agg_input['codiceFiscale'].values

             
        }

    awards_suppliers = pd.DataFrame(awards_suppliers_data, columns = ['ocid', 
                                                    'id', 
                                                    'awards/0/id',
                                                    'awards/0/suppliers/0/name',
                                                    'awards/0/suppliers/0/id'
                                                    ])


    return awards_suppliers


def create_contr_imple_transactions (lot_input):

    contr_imple_transactions_data = {
             'ocid': lot_input['ocid'].values,
             'id': lot_input['cig'].values,
             'contracts/0/id': lot_input['cig'].values,
             'contracts/0/implementation/transactions/0/id': lot_input['cig'].values,
             'contracts/0/implementation/transactions/0/value/amount': lot_input['importoSommeLiquidate'].values

             
        }

    contr_imple_transactions = pd.DataFrame(contr_imple_transactions_data, columns = ['ocid', 
                                                    'id', 
                                                    'contracts/0/id',
                                                    'contracts/0/implementation/transactions/0/id',                              
                                                    'contracts/0/implementation/transactions/0/value/amount'
                                                    ])


    return contr_imple_transactions


def create_contracts (agg_input):

    contracts_data = {
            'ocid': agg_input['ocid'].values,
            'id': agg_input['cig'].values,
            #'contracts/0/awardID': agg_input.index.values.astype(int),
            'contracts/0/awardID': agg_input['cig'].values,
            'contracts/0/id':agg_input['cig'].values
             
        }

    contracts = pd.DataFrame(contracts_data, columns = ['ocid',
                            'id',
                                'contracts/0/awardID',
                                'contracts/0/id'])



    return contracts





   