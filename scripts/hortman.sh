#!/bin/bash
if [[ -f ~/.profile ]] 
then
	. ~/.profile
	else
	if [[ -f ~/.bash_profile ]]
	then
		. ~/.bash_profile
		else
		if [[ -f ~/.bashrc ]]
		then
			. ~/.bashrc
			else
				echo No '.profile' files found. Initialisation falied.
				exit 1
		fi 
	fi
fi


if [[ -z $SRVHOME ]]; then
	echo "ENV variable \$SRVHOME must be set"
	exit 1
fi

if [[ -z $ARCHOME ]]; then
	echo "ENV variable \$ARCHOME must be set"
	exit 1
fi

if [[ $1 == "kksfile" ]]
then
	kks_dir="$ARCHOME/$2"
	i=0
	for syn in "$@"
	do
		let i=i+1
		if [[ $i -ge 3 ]]
		then
			if [[ -f "$kks_dir/$syn.kks" ]]
			then
				echo "$syn.kks"
				exit 0
			fi
		fi
	done
	echo ""
	exit 1
fi
 
if [[ $1 == "kks" ]]
then
	file="$ARCHOME/$2"
	if [[ -f $file ]]
	then
		cat $file
		exit 0
	else 
		echo ""
		exit 1
	fi
fi

if [[ $1 == "clean" ]]
then
	rm -Rf $SRVHOME/ini/*
	rm -Rf $SRVHOME/out/*
	rm -Rf $SRVHOME/log/*
	exit 0
fi

task=$1
mkdir -p $SRVHOME/out/$task
mkdir -p $SRVHOME/pid

ln -s $SRVHOME/out/$task/$task.log $SRVHOME/out/$task/log.txt

LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$SRVHOME"
#echo $$ > $SRVHOME/pid/$task.pid
cd $SRVHOME
export LC_NUMERIC=C
LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$SRVHOME" $SRVHOME/ImGHort $task  >  $SRVHOME/out/$task/output.txt
#rm -f $SRVHOME/pid/$task.pid

