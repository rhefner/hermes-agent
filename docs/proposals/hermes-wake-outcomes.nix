# PROPOSAL ONLY. Coordinator imports/enables after independent source approval.
# Does not start/restart the gateway or manage any named profile.
{ config, lib, ... }:
let
  cfg = config.services.hermesWakeOutcomes;
  home = "${config.home.homeDirectory}/.hermes";
  repo = "${home}/hermes-agent";
in {
  options.services.hermesWakeOutcomes.enable = lib.mkEnableOption "default-only Hermes wake receipt reconciliation";
  config = lib.mkIf cfg.enable {
    systemd.user.services.hermes-wake-outcomes = {
      Unit = {
        Description = "Hermes default wake receipt reconciliation (model-free)";
        After = [ "network-online.target" ];
      };
      Service = {
        Type = "oneshot";
        WorkingDirectory = home;
        Environment = [ "HERMES_HOME=${home}" "HERMES_PROFILE=default" ];
        ExecStart = "${repo}/venv/bin/python ${repo}/scripts/wake-monitor-default.py";
        TimeoutStartSec = 120;
        UMask = "0077";
        NoNewPrivileges = true;
        Restart = "no";
      };
    };
    systemd.user.timers.hermes-wake-outcomes = {
      Unit.Description = "Finite default wake receipt reconciliation";
      Timer = {
        OnBootSec = "1min";
        OnUnitInactiveSec = "1min";
        AccuracySec = "10s";
        Unit = "hermes-wake-outcomes.service";
      };
      Install.WantedBy = [ "timers.target" ];
    };
  };
}
