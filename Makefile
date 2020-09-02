SUBDIRS := cmd/admin/ cmd/dumpwal/ worker/ zero/

all: $(SUBDIRS)
$(SUBDIRS):
	$(MAKE) -C $@

.PHONY: all $(SUBDIRS)
