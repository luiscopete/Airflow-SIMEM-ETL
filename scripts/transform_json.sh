#################################
# Author: Luis Copete			#
# Role: Data Engineer			#
# Linkedin: in/luiscopete		#
#################################

# JSON File
json_file="./RAW/$1.json"

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

# Imprimir los nombres de las columnas
echo "$column_names"

# Imprimir la tabla
echo "$csv"

#echo "$table"

