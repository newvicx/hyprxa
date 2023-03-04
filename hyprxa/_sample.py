from hyprxa.application import Hyprxa



event_hub_app = Hyprxa(debug=True, include_timeseries=False)
timeseries_app = Hyprxa(debug=True, include_event_hub=False)
app = Hyprxa(debug=True)