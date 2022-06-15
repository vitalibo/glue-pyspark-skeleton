environment := dev
profile := default
auto_approve := false
version := 0

define tfvar
$(shell sed -n 's/^$(1) *= *"\(.*\)"$$/\1/p' $(CURDIR)/vars/$(environment).tfvars)
endef

help:
	@echo "Usage: make <plan|apply|destroy> environment=<string> profile=<string> auto_approve=<true|false>"
	@echo " "
	@echo "Options:"
	@echo "  environment      The terraform input variables file name (Supported values: [$(ENVIRONMENTS)])."
	@echo "  profile          Use a specific AWS profile from your credential file (Default: '$(profile)')."
	@echo "  auto_approve     Skip interactive approval of plan before applying (Default: '$(auto_approve)')."
	@echo " "

configure:
    ENVIRONMENTS := $(shell find ./vars -type f -name '*.tfvars' | cut -c 8- | rev | cut -c 8- | rev | paste -sd " " - | xargs)
    ifeq ($(filter $(environment),$(ENVIRONMENTS)),)
        $(error Environment '$(environment)' is not supported)
    endif

    TF_ENV_VARS := $(TF_ENV_VARS) TF_DATA_DIR="$(CURDIR)/.terraform/$(environment)"
    # TF_ENV_VARS := $(TF_ENV_VARS) TF_LOG=DEBUG

    TF_OPTIONS := $(TF_OPTIONS) -var="environment=$(environment)"
    TF_OPTIONS := $(TF_OPTIONS) -var="profile=$(profile)"
    TF_OPTIONS := $(TF_OPTIONS) -var="script_home=s3://$(call tfvar,bucket_name)/$(environment)/$(call tfvar,name)/$(version)"

    TF_INIT_CONF := $(TF_INIT_CONF) -backend-config="bucket=$(call tfvar,bucket_name)"
    TF_INIT_CONF := $(TF_INIT_CONF) -backend-config="key=terraform/state/$(environment)/$(call tfvar,name).tfstate"
    TF_INIT_CONF := $(TF_INIT_CONF) -backend-config="profile=$(profile)"
    TF_INIT_CONF := $(TF_INIT_CONF) -backend-config="region=$(call tfvar,region)"
    TF_INIT_CONF := $(TF_INIT_CONF) -var-file="$(CURDIR)/vars/$(environment).tfvars"
    # TF_INIT_CONF := $(TF_INIT_CONF) -reconfigure

    TF_RUN_CONF := $(TF_RUN_CONF) -var-file="$(CURDIR)/vars/$(environment).tfvars"
    ifeq ($(auto_approve),true)
        TF_RUN_CONF := $(TF_RUN_CONF) -auto-approve
    endif

    TF_OUT_CONF := $(TF_OUT_CONF) -json

init: configure
	$(TF_ENV_VARS) terraform init $(TF_OPTIONS) $(TF_INIT_CONF)

plan: init
	$(TF_ENV_VARS) terraform plan $(TF_OPTIONS) $(TF_RUN_CONF)

apply: init
	$(TF_ENV_VARS) terraform apply $(TF_OPTIONS) $(TF_RUN_CONF)

destroy: init
	$(TF_ENV_VARS) terraform destroy $(TF_OPTIONS) $(TF_RUN_CONF)

output:
	@$(TF_ENV_VARS) terraform output $(TF_OUT_CONF)

.PHONY: help init plan apply destroy output
