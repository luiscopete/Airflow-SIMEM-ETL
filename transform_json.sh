#################################
# Author: Luis Copete			#
# Role: Data Engineer			#
# Linkedin: in/luiscopete		#
#################################

# JSON File
json_file="./RAW/$1.json"

# Transform JSON to table
table=$(jq -r '
  ["FechaPublicacion", "FechaInicioCargo", "FechaFinCargo", "CodigoSICAgente", "CodigoPlanta", "Tecnologia", "OEF"],
  (.result.records[] | [.FechaPublicacion, .FechaInicioCargo, .FechaFinCargo, .CodigoSICAgente, .CodigoPlanta, .Tecnologia, .OEF])
  | @csv' "$json_file")

echo "$table"

