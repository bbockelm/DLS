#!/bin/bash

PYXML_VER=PyXML-0.8.4

echo "Extracting PyXML archive"
tar xvzf ./$PYXML_VER.tar.gz >/dev/null;

echo "Compiling PyXML binary objects"
cd ./$PYXML_VER;
python setup.py build >/dev/null;
cd ..
#python setup.py install >/dev/null;
echo "Installing PyXML in DLSAPI folder"
mkdir xml;
cp -r $PYXML_VER/build/lib.*/_xmlplus xml/.;
mv xml/_xmlplus/* xml/.;
rmdir xml/_xmlplus;

echo "PyXML installation performed"
rm -Rf $PYXML_VER.tar.gz $PYXML_VER

