#!/bin/bash

#################################
# Author: Luis Copete			#
# Role: Data Engineer			#
# Linkedin: in/luiscopete		#
#################################

#file_path = $1
#datasetid = $2
#output_path = $3
#output_file_name = $4

# JSON File
json_file="$1/$2.json"

echo $1
echo $2
echo $3
echo $4

# Transform JSON to table
#table=$(jq -r '
#  ["FechaPublicacion", "FechaInicioCargo", "FechaFinCargo", "CodigoSICAgente", "CodigoPlanta", "Tecnologia", "OEF"],
#  (.result.records[] | [.FechaPublicacion, .FechaInicioCargo, .FechaFinCargo, .CodigoSICAgente, .CodigoPlanta, .Tecnologia, .OEF])
#  | @csv' "$json_file")


# Extraer los nombres de las columnas del primer registro
column_names=$(jq -r '.result.records[0] | keys_unsorted | @csv' "$json_file")

# Convertir el JSON a formato CSV
csv=$(jq -r '
  (.result.records[] | to_entries | map(.value) | @csv)
  ' "$json_file")


# save table in a file
echo "$csv" > "$3/$4"


