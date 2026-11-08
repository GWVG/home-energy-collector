option task = {name: "downsample_telemetrie_15m", every: 15m, offset: 1m}

// Rolls raw data up into 15-minute means for long-term storage.
//
// Every 15 minutes, this processes the 15-minute slice that has just turned
// seven days old. Raw data is kept for 90 days, so if the task stalls there
// is ample time to notice and backfill.
//
// It originally ran once a day over a full day of data (every: 1d,
// range(-8d, -7d)). That meant aggregating ~13,000 series in one run, which
// filled the swap of the 2 GB container. The same daily work split into 96
// small runs has a ~96x smaller memory peak per run.
//
// Only an allowlist of numeric fields is downsampled: mean() fails on
// strings and booleans, and Flux has no clean way to filter on type at
// runtime. An allowlist fails closed: a new numeric field is skipped until
// it is added here, but a new string or boolean field can never crash the
// task. 15-minute windows (rather than the usual 1 hour) match the grid
// operator's own peak-demand windows, which some dashboards rely on.

numericFields = [
    "accumulated_yield_energy", "acpitz-acpi-0_temp1_temp1_input", "active_current_a", "active_current_l1_a",
    "active_current_l2_a", "active_current_l3_a", "active_power", "active_power_average_w",
    "active_power_l1_w", "active_power_l2_w", "active_power_l3_w", "active_power_w",
    "active_tariff", "active_voltage_l1_v", "active_voltage_l2_v", "active_voltage_l3_v",
    "available_kib", "capacity_blocks", "capacity_bytes", "coretemp-isa-0000_core 0_temp2_crit",
    "coretemp-isa-0000_core 0_temp2_crit_alarm", "coretemp-isa-0000_core 0_temp2_input", "coretemp-isa-0000_core 0_temp2_max", "coretemp-isa-0000_core 12_temp14_crit",
    "coretemp-isa-0000_core 12_temp14_crit_alarm", "coretemp-isa-0000_core 12_temp14_input", "coretemp-isa-0000_core 12_temp14_max", "coretemp-isa-0000_core 16_temp18_crit",
    "coretemp-isa-0000_core 16_temp18_crit_alarm", "coretemp-isa-0000_core 16_temp18_input", "coretemp-isa-0000_core 16_temp18_max", "coretemp-isa-0000_core 17_temp19_crit",
    "coretemp-isa-0000_core 17_temp19_crit_alarm", "coretemp-isa-0000_core 17_temp19_input", "coretemp-isa-0000_core 17_temp19_max", "coretemp-isa-0000_core 18_temp20_crit",
    "coretemp-isa-0000_core 18_temp20_crit_alarm", "coretemp-isa-0000_core 18_temp20_input", "coretemp-isa-0000_core 18_temp20_max", "coretemp-isa-0000_core 19_temp21_crit",
    "coretemp-isa-0000_core 19_temp21_crit_alarm", "coretemp-isa-0000_core 19_temp21_input", "coretemp-isa-0000_core 19_temp21_max", "coretemp-isa-0000_core 20_temp22_crit",
    "coretemp-isa-0000_core 20_temp22_crit_alarm", "coretemp-isa-0000_core 20_temp22_input", "coretemp-isa-0000_core 20_temp22_max", "coretemp-isa-0000_core 21_temp23_crit",
    "coretemp-isa-0000_core 21_temp23_crit_alarm", "coretemp-isa-0000_core 21_temp23_input", "coretemp-isa-0000_core 21_temp23_max", "coretemp-isa-0000_core 22_temp24_crit",
    "coretemp-isa-0000_core 22_temp24_crit_alarm", "coretemp-isa-0000_core 22_temp24_input", "coretemp-isa-0000_core 22_temp24_max", "coretemp-isa-0000_core 23_temp25_crit",
    "coretemp-isa-0000_core 23_temp25_crit_alarm", "coretemp-isa-0000_core 23_temp25_input", "coretemp-isa-0000_core 23_temp25_max", "coretemp-isa-0000_core 4_temp6_crit",
    "coretemp-isa-0000_core 4_temp6_crit_alarm", "coretemp-isa-0000_core 4_temp6_input", "coretemp-isa-0000_core 4_temp6_max", "coretemp-isa-0000_core 8_temp10_crit",
    "coretemp-isa-0000_core 8_temp10_crit_alarm", "coretemp-isa-0000_core 8_temp10_input", "coretemp-isa-0000_core 8_temp10_max", "coretemp-isa-0000_package id 0_temp1_crit",
    "coretemp-isa-0000_package id 0_temp1_crit_alarm", "coretemp-isa-0000_package id 0_temp1_input", "coretemp-isa-0000_package id 0_temp1_max", "daily_yield_energy",
    "data_bytes", "day_active_power_peak", "dewpoint_c", "efficiency",
    "endurance_used_current_percent", "entry_latency_us", "eui64_ext_id", "eui64_oui",
    "exit_latency_us", "features_other", "features_value", "formatted_lba_size",
    "general_asso_val", "general_board_time_val", "general_identify_val", "general_lan_rssiwifi_val",
    "general_modbus_writereqcntremain_val", "general_parent_val", "general_publicapi_writereqcntremain_val", "general_subtype_val",
    "grid_frequency", "heatrecovery_general_timefilterremain_val", "hourly_yield_energy", "humidity_pct",
    "id", "input_power", "insulation_resistance", "internal_temperature",
    "line_voltage_a_b", "line_voltage_b_c", "line_voltage_c_a", "local_time_time_t",
    "logical_block_size", "max_power_scale", "max_power_units_per_watt", "max_power_value",
    "metadata_bytes", "metadata_change", "metadata_decimal", "metadata_divisor",
    "metadata_intdefaultvalue", "metadata_maxvalue", "metadata_minvalue", "metadata_modbusregisterid",
    "metadata_variableid", "meter_status", "meter_type", "model_id",
    "monthly_yield_energy", "montly_power_peak_timestamp", "montly_power_peak_w", "mt7921_phy0-pci-5900_temp1_temp1_input",
    "mt7921_phy0-pci-5a00_temp1_temp1_input", "nb_mpp_tracks", "nb_optimizers", "nb_pv_strings",
    "node", "nvme-pci-0100_composite_temp1_alarm", "nvme-pci-0100_composite_temp1_crit", "nvme-pci-0100_composite_temp1_input",
    "nvme-pci-0100_composite_temp1_max", "nvme-pci-0100_composite_temp1_min", "nvme-pci-0100_sensor 1_temp2_input", "nvme-pci-0100_sensor 1_temp2_max",
    "nvme-pci-0100_sensor 1_temp2_min", "nvme-pci-5800_composite_temp1_alarm", "nvme-pci-5800_composite_temp1_crit", "nvme-pci-5800_composite_temp1_input",
    "nvme-pci-5800_composite_temp1_max", "nvme-pci-5800_composite_temp1_min", "nvme-pci-5800_sensor 1_temp2_input", "nvme-pci-5800_sensor 1_temp2_max",
    "nvme-pci-5800_sensor 1_temp2_min", "nvme_composite_temperature_threshold_critical", "nvme_composite_temperature_threshold_warning", "nvme_controller_id",
    "nvme_error_information_log_read", "nvme_error_information_log_size", "nvme_error_information_log_unread", "nvme_firmware_update_capabilities_other",
    "nvme_firmware_update_capabilities_slots", "nvme_firmware_update_capabilities_value", "nvme_ieee_oui_identifier", "nvme_log_page_attributes_other",
    "nvme_log_page_attributes_value", "nvme_maximum_data_transfer_pages", "nvme_number_of_namespaces", "nvme_optional_admin_commands_other",
    "nvme_optional_admin_commands_value", "nvme_optional_nvm_commands_other", "nvme_optional_nvm_commands_value", "nvme_pci_vendor_id",
    "nvme_pci_vendor_subsystem_id", "nvme_self_test_log_current_self_test_operation_value", "nvme_self_test_log_nsid", "nvme_smart_health_information_log_available_spare",
    "nvme_smart_health_information_log_available_spare_threshold", "nvme_smart_health_information_log_controller_busy_time", "nvme_smart_health_information_log_critical_comp_time", "nvme_smart_health_information_log_critical_warning",
    "nvme_smart_health_information_log_data_units_read", "nvme_smart_health_information_log_data_units_written", "nvme_smart_health_information_log_host_reads", "nvme_smart_health_information_log_host_writes",
    "nvme_smart_health_information_log_media_errors", "nvme_smart_health_information_log_nsid", "nvme_smart_health_information_log_num_err_log_entries", "nvme_smart_health_information_log_percentage_used",
    "nvme_smart_health_information_log_power_cycles", "nvme_smart_health_information_log_power_on_hours", "nvme_smart_health_information_log_temperature", "nvme_smart_health_information_log_unsafe_shutdowns",
    "nvme_smart_health_information_log_warning_temp_time", "nvme_total_capacity", "nvme_unallocated_capacity", "nvme_version_value",
    "operative_temp_to", "percent_used", "phase_a_current", "phase_b_current",
    "phase_c_current", "pmv", "power_cycle_count", "power_factor",
    "power_meter_active_power", "power_meter_reactive_power", "power_on_time_hours", "ppd",
    "pressure_hpa", "pv_01_current", "pv_01_voltage", "pv_02_current",
    "pv_02_voltage", "rated_power", "reactive_power", "relative_performance",
    "relative_read_latency", "relative_read_throughput", "relative_write_latency", "relative_write_throughput",
    "rh_pct", "room_temp", "scaled_value", "sensor_iaqrh_val",
    "sensor_rh_val", "size_blocks", "size_bytes", "smart_status_nvme_value",
    "smartctl_exit_status", "smr_version", "spare_available_current_percent", "spare_available_threshold_percent",
    "temp_c", "temperature_critical_limit_max", "temperature_current", "temperature_op_limit_max",
    "timestamp", "total_dc_input_power", "total_kib", "total_power_export_kwh",
    "total_power_export_t1_kwh", "total_power_export_t2_kwh", "total_power_import_kwh", "total_power_import_t1_kwh",
    "total_power_import_t2_kwh", "used_kib", "user_capacity_blocks", "user_capacity_bytes",
    "utilization_blocks", "utilization_bytes", "value", "value_integervalue",
    "value_variableid", "ventilation_flowlvltgt_val", "ventilation_sensor_tempeha_val", "ventilation_sensor_tempeta_val",
    "ventilation_sensor_tempoda_val", "ventilation_sensor_tempsup_val", "ventilation_timestateend_val", "ventilation_timestateremain_val",
    "wifi_strength", "yearly_yield_energy",
]

from(bucket: "telemetrie")
  |> range(start: -7d15m, stop: -7d)
  |> filter(fn: (r) => contains(value: r._field, set: numericFields))
  |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
  |> to(bucket: "telemetrie_downsampled", org: "huis")

