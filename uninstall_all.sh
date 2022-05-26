#!/usr/bin/env bash
# A shell script that will un-install all python pakges.

echo "-------------------------------------------------"
echo "------ Un-Install All Python Requirements -------"
echo "-------------------------------------------------"

UNINSTALL_FILE="./un_install_$(date -d "today" +"%Y%m%d%H%M").txt"

pip freeze > "$UNINSTALL_FILE"
echo "Un-install file created..."

pip uninstall -r "$UNINSTALL_FILE" -y

rm "$UNINSTALL_FILE"
echo "Un-install file removed..."

echo "----------------------------------------------"
echo " - un-installation complete"
echo "----------------------------------------------"

<<<<<<< HEAD
read -p -r "Press any key to end ..."
=======
read -p -r "Press any key to end ..."
>>>>>>> 21537ae1b5d8d43e3527021afe65a0b176911617
