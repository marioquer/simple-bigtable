include hosts.mk

# EDIT THIS
MASTER_CMD=python3 -m venv bigtable/venv; source bigtable/venv/bin/activate; pip3 install flask; pip3 install requests; python3 bigtable/master/router.py
TABLET_CMD=python3 -m venv bigtable/venv; source bigtable/venv/bin/activate; pip3 install flask; pip3 install requests; python3 bigtable/tablet/router.py
# END EDIT REGION

# if you require any compilation, fill in this section
compile:
	echo "no compile"

grade1:
	python3 grading/grading.py 1 $(TABLET1_HOSTNAME) $(TABLET1_PORT)

grade2:
	python3 grading/grading.py 2 $(MASTER_HOSTNAME) $(MASTER_PORT)

master:
	$(MASTER_CMD) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet1:
	$(TABLET_CMD) $(TABLET1_HOSTNAME) $(TABLET1_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)  $(WAL) $(SSTABLE_FOLDER)

tablet2:
	$(TABLET_CMD) $(TABLET2_HOSTNAME) $(TABLET2_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)  $(WAL) $(SSTABLE_FOLDER)

tablet3:
	$(TABLET_CMD) $(TABLET3_HOSTNAME) $(TABLET3_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)  $(WAL) $(SSTABLE_FOLDER)

.PHONY: master tablet1 tablet2 tablet3 grade1 grade2 compile

