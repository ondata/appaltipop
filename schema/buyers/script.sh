#!/bin/bash

### requirements ###
# scrivere
### requirements ###

set -x

folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$folder"/resources

### dati IPA ###

scaricadatiIPA="yes"
URLAmministrazioniIPA="https://www.indicepa.gov.it/public-services/opendata-read-service.php?dstype=FS&filename=amministrazioni.txt"

if [[ $scaricadatiIPA == "yes" ]]; then
  curl -L "$URLAmministrazioniIPA" >"$folder"/resources/amministrazioni.tsv
fi

# correggi spazi bianchi e converti in CSV
mlr --icsvlite --ifs "\t" --ocsv clean-whitespace "$folder"/resources/amministrazioni.tsv >"$folder"/resources/amministrazioni.csv

### comuni IPA  ###

# estrai dati sui comuni
mlr --csv filter '$tipologia_istat=="Comuni e loro Consorzi e Associazioni" && $cod_amm=~"^c_" && $des_amm=~"^Comun"' \
  then put '$nomeComune=gsub($des_amm,"^(Comune )(di )*(.+)$","\3")' \
  then reorder -f cod_amm,nomeComune "$folder"/resources/amministrazioni.csv >"$folder"/resources/comuniIPA.csv

### dati comuni ANPR ###

scaricadatiANPR="yes"
comuniANPR="https://www.anpr.interno.it/portale/anpr-archivio-comuni.csv"
if [[ $scaricadatiIPA == "yes" ]]; then
  curl -L "$comuniANPR" >"$folder"/resources/comuniANPR.csv
fi

# estrai comuni attivi
mlr --csv filter '$STATO=="A"' "$folder"/resources/comuniANPR.csv >"$folder"/resources/comuniANPRattivi.csv

### dati comuni ISTAT ###

scaricadatiISTAT="yes"
comuniISTAT="https://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.csv"
if [[ $scaricadatiIPA == "yes" ]]; then
  curl -L "$comuniISTAT" >"$folder"/resources/comuniISTAT_raw.csv
fi
# cambia codifica in UTF-8
iconv -f WINDOWS-1252 -t UTF-8 "$folder"/resources/comuniISTAT_raw.csv >"$folder"/resources/comuniISTAT.csv
# correggi spazi bianchi
mlr -I --csv --ifs ";" clean-whitespace "$folder"/resources/comuniISTAT.csv
# rimuovi ritorno a capo dai titoli
mlr -I --csv -N put -S 'if (NR == 1) {for (k in $*) {$[k] = clean_whitespace(gsub($[k], "\n", " "))}}' "$folder"/resources/comuniISTAT.csv
# imposta formato numerico corretto per popolazione
mlr -I --csv put -S '${Popolazione legale 2011 (09/10/2011)}=gsub(${Popolazione legale 2011 (09/10/2011)},"\.","")' "$folder"/resources/comuniISTAT.csv

### Associa codice comunale ISTAT a dati IPA ###

# estrai codice regione e etichetta e estrai valori univoci
mlr --csv -N cut -f 1,11 then uniq -a "$folder"/resources/comuniISTAT.csv >"$folder"/resources/regioniISTAT.csv
# estrai codici regioni da ISTAT e normalizza i valori così per come in IPA
mlr -I --csv put '${regione}=gsub(${Denominazione regione},"/.*","");${regione}=gsub(${regione},"-"," ")' "$folder"/resources/regioniISTAT.csv

# associa etichetta regione a codice ISTAT
csvmatch -i -a -n "$folder"/resources/comuniANPRattivi.csv "$folder"/resources/regioniISTAT.csv --fields1 "IDREGIONE" --fields2 "Codice Regione" --join left-outer --output 1.IDREGIONE 2.regione >"$folder"/resources/regioniANPR.csv
# rimuovi righe vuote
sed -i '/^$/d' "$folder"/resources/regioniANPR.csv
# estrai valori univoci
mlr -I --csv uniq -a "$folder"/resources/regioniANPR.csv
# fai il join tra comuni ANPR e file con nomi regioni per codici
mlr --csv join --ul -j IDREGIONE -f "$folder"/resources/comuniANPRattivi.csv then unsparsify "$folder"/resources/regioniANPR.csv >"$folder"/resources/tmp.csv
mv "$folder"/resources/tmp.csv "$folder"/resources/comuniANPRattivi.csv

# Estrai coppia nome comune IPA, nome regione ANPR via JOIN
csvmatch -i -a -n "$folder"/resources/comuniIPA.csv "$folder"/resources/comuniANPRattivi.csv --fields1 "nomeComune" Regione --fields2 "DENOMTRASLITTERATA" regione --join left-outer --output 1.nomeComune 2.CODISTAT 2.regione >"$folder"/resources/codiciISTATcomuniIPA.csv
sed -i '/^$/d' "$folder"/resources/codiciISTATcomuniIPA.csv

# Estrai coppia codice amministrazione, codice comune ISTAT, via JOIN
csvmatch -i -a -n "$folder"/resources/comuniIPA.csv "$folder"/resources/comuniANPRattivi.csv --fields1 "nomeComune" Regione --fields2 "DENOMTRASLITTERATA" regione --join left-outer --output 1.cod_amm 2.CODISTAT >"$folder"/resources/codiciISTATcomuniIPA.csv
sed -i '/^$/d' "$folder"/resources/codiciISTATcomuniIPA.csv

# associa ai dati IPA il codice comunale
mlr --csv join --ul -j cod_amm -f "$folder"/resources/comuniIPA.csv then unsparsify "$folder"/resources/codiciISTATcomuniIPA.csv >"$folder"/resources/tmp.csv
mv "$folder"/resources/tmp.csv "$folder"/resources/comuniIPA.csv

### Aggiungi a IPA i dati sulla popolazione ###

# associa ai dati IPA il numero di abitanti
mlr --csv cut -f "Codice Comune formato alfanumerico","Popolazione legale 2011 (09/10/2011)" \
  then label CODISTAT,popolazione "$folder"/resources/comuniISTAT.csv >"$folder"/resources/comuniPopolazione.csv

mlr --csv join --ul -j CODISTAT -f "$folder"/resources/comuniIPA.csv then unsparsify "$folder"/resources/comuniPopolazione.csv >"$folder"/resources/tmp.csv
mv "$folder"/resources/tmp.csv "$folder"/resources/comuniIPA.csv

mlr --csv cut -f CODISTAT,cod_amm,nomeComune,des_amm,Cap,Provincia,Regione,sito_istituzionale,Indirizzo,Cf,popolazione "$folder"/resources/comuniIPA.csv >"$folder"/resources/anagraficaBuyers.csv

mlr --csv cut -f "Codice Regione","Codice dell'Unità territoriale sovracomunale (valida a fini statistici)","Codice Comune formato alfanumerico","Denominazione in italiano","Denominazione regione","Denominazione dell'Unità territoriale sovracomunale (valida a fini statistici)","Sigla automobilistica","Codice Catastale del comune" "$folder"/resources/comuniISTAT.csv >"$folder"/resources/tmp.csv

mlr --csv join --ul -j CODISTAT -l CODISTAT -r "Codice Comune formato alfanumerico" -f "$folder"/resources/anagraficaBuyers.csv then unsparsify then filter -S -x '$CODISTAT==""' "$folder"/resources/tmp.csv >"$folder"/anagraficaBuyers.csv

# rimuovi provincia e regione
mlr -I --csv cut -x -f Provincia,Regione "$folder"/anagraficaBuyers.csv

mlr -I --csv cut -x -f nomeComune,popolazione,des_amm,"Codice Catastale del comune" then rename CODISTAT,PRO_COM_T,"Codice Regione",COD_REG,"Codice dell'Unità territoriale sovracomunale (valida a fini statistici)",COD_CM,"Denominazione regione",DEN_REG,"Denominazione dell'Unità territoriale sovracomunale (valida a fini statistici)",DEN_CM,"Sigla automobilistica",SIGLA,cod_amm,IPA:cod,Cap,IPA:CAP,sito_istituzionale,IPA:sitoWeb,Cf,CF,"Denominazione in italiano",COMUNE,Indirizzo,IPA:indirizzo then put -S '$CF="IT-CF-".$CF' "$folder"/anagraficaBuyers.csv

mlr --csv join --ul -j id -l id -r CF -f "$folder"/resources/tmp_list.csv then unsparsify "$folder"/anagraficaBuyers.csv >"$folder"/tmp.csv

mv "$folder"/tmp.csv "$folder"/anagraficaBuyers.csv

mlr -I --csv rename id,"ocds:releases/0/buyer/id",name,"ocds:releases/0/buyer/name",PRO_COM_T,Istat:PRO_COM_T,IPA:CAP,"ocds:releases/0/parties/address/postalCode",IPA:sitoWeb,"ocds:releases/0/parties/contactPoint/url",IPA:indirizzo,"ocds:releases/0/parties/address/streetAddress",COD_REG,"Istat:COD_REG","COD_CM","Istat:COD_CM","COMUNE","Istat:COMUNE","DEN_REG","ocds:releases/0/parties/address/region","DEN_CM","Istat:DEN_CM","SIGLA","licencePlateCode" "$folder"/anagraficaBuyers.csv

mlr --c2j cat "$folder"/anagraficaBuyers.csv >"$folder"/anagraficaBuyers.json
