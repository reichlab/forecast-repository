//
// helper functions
//

// `updateModelsList()` helper
function _selectModelDiv(model, modelColor, isEnabled, isChecked) {
    const disabled = isEnabled ? '' : 'disabled="disabled"';
    const checked = isChecked ? 'checked' : '';
    return `<div class="form-group form-check"
                 style="margin-bottom: 0${!isEnabled ? '; color: lightgrey' : ''}">
                <label>
                    <input type="checkbox" id="${model}" class="model-check" ${disabled} ${checked}>
                    &nbsp;${model}
                    &nbsp;<span class="forecastViz_dot" style="background-color: ${modelColor}; "></span>
                </label>
            </div>`;
}


// event handler helper
function _setSelectedTruths() {
    const isCurrTruthChecked = $("#forecastViz_Current_Truth").prop('checked');
    const isAsOfTruthChecked = $("#forecastViz_Truth_as_of").prop('checked');  // ""
    const selectedTruths = [];
    if (isCurrTruthChecked) {
        selectedTruths.push('Current Truth');
    }
    if (isAsOfTruthChecked) {
        selectedTruths.push('Truth as of');
    }
    App.state.selected_truth = selectedTruths;
    App.fetchDataUpdatePlot(false, null);
}


/***
 * Makes an AJAX call to the Zoltar API to fetch truth or forecast data using the passed args, which correspond to those
 * of `utils.visualization.viz_data()` (see). The `success` and `error` callbacks are passed directly to the $.ajax()
 * call and therefore accept the standard arguments:
 * - success: function (data, textStatus, jqXHR) { ... }
 * - error: function (jqXHR, textStatus, thrownError) { ... }
 */
function _fetchData(isForecast, targetKey, unitAbbrev, referenceDate) {
    const url = "/api/project/" + App.projectId + "/viz-data/";
    const requestData = {
        is_forecast: isForecast,
        target_key: targetKey,
        unit_abbrev: unitAbbrev,
        reference_date: referenceDate,
    };

    // using JQuery ajax()
    // return $.ajax({url: url, type: 'GET', data: requestData, dataType: 'json', success: success, error: error,});

    // using https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
    const urlObj = new URL(url, document.location.origin);
    Object.keys(requestData).forEach(key => urlObj.searchParams.append(key, requestData[key]))
    return fetch(urlObj);  // Promise
}


// helper for `_fetchData()` error arg
function _handleFetchDataError(jqXHR, textStatus, thrownError) {
    console.log("_handleFetchDataError(): error. textStatus=" + textStatus + ", thrownError=" + thrownError);
    // todo xx
}


//
// App
//

// this implements a straightforward SPA with state - based on https://dev.to/vijaypushkin/dead-simple-state-management-in-vanilla-javascript-24p0
const App = {
    projectId: -1,  // set by initialize()


    //
    // the app's state
    //

    state: {
        // Static data, fixed at time of creation
        target_variables: [],
        locations: [],
        intervals: [],
        available_as_ofs: [],
        current_date: "",
        models: [],
        initial_checked_models: [],
        disclaimer: "",

        // Dynamic/updated and we need to track: 2 categories:
        // 1/2 Tracks UI state:
        selected_target_var: '',
        selected_location: '',
        selected_interval: '',
        selected_as_of_date: '',
        selected_truth: ['Current Truth', 'Truth as of'],
        selected_models: [],
        last_selected_models: [],  // last manually-selected models. used by "Select Models" checkbox
        colors: [],

        // 2/2 Data used to create plots:
        current_truth: [],
        as_of_truth: [],
        forecasts: {},
    },


    //
    // initialization-related functions
    //

    initialize(projectId, options) {
        App.projectId = projectId;
        console.log('initialize(): entered', projectId);

        /*
        // todo xx all this authorization stuff needs careful thinking:
        // configure AJAX to work with DRF - per https://stackoverflow.com/questions/42514560/django-and-ajax-csrf-token-missing-despite-being-passed
        function csrfSafeMethod(method) {
            return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
        }

        $.ajaxSetup({
            beforeSend: function (xhr, settings) {
                if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
                    xhr.setRequestHeader("X-CSRFToken", csrf_token);
                }
            }
        });
        */

        // save static vars
        this.state.target_variables = options['target_variables'];
        this.state.locations = options['units'];
        this.state.intervals = options['intervals'];
        this.state.available_as_ofs = options['available_as_ofs'];
        this.state.current_date = options['current_date'];
        this.state.models = options['models'];
        this.state.initial_checked_models = options['initial_checked_models'];
        this.state.disclaimer = options['disclaimer'];
        this.state.colors = Array(parseInt(this.state.models.length / 10, 10) + 1).fill([
            '#0d0887',
            '#46039f',
            '#7201a8',
            '#9c179e',
            '#bd3786',
            '#d8576b',
            '#ed7953',
            '#fb9f3a',
            '#fdca26',
            '#f0f921'
        ]).flat()

        // save initial selected state
        this.state.selected_target_var = options['initial_target_var'];
        this.state.selected_location = options['initial_unit'];
        this.state.selected_interval = options['init_interval'];
        this.state.selected_as_of_date = options['current_date'];
        // this.state.selected_truth: synchronized via default <input ... checked> setting
        this.state.selected_models = options['initial_checked_models'];

        /*
        const debugObj = {
            'target_variables': this.state.target_variables,
            'locations': this.state.locations,
            'intervals': this.state.intervals,
            'available_as_ofs': this.state.available_as_ofs,
            'current_date': this.state.current_date,
            'models': this.state.models,
            'colors': this.state.colors,
        };
        console.log('initialize(): static vars initialized', JSON.stringify(debugObj));
        */

        // populate UI elements, setting selection state to initial
        console.log('initialize(): initializing UI');
        this.initializeUI();

        // wire up UI controls (event handlers)
        this.addEventHandlers();

        // pull initial data (current truth, selected truth, and selected forecast) and update the plot
        console.log('initialize(): fetching data and updating plot');
        this.fetchDataUpdatePlot(true, true);

        console.log('initialize(): done');
    },
    initializeUI() {
        // populate options and models list (left column)
        this.initializeTargetVarsUI();
        this.initializeUnitsUI();
        this.initializeIntervalsUI();
        this.updateModelsList();

        // initialize current and as_of truth checkboxes' text
        $("#currentTruthDate").text(`Current (${this.state.current_date})`);
        this.updateTruthAsOfCheckboxText();

        // initialize disclaimer
        $('#disclaimer').text(this.state.disclaimer);

        // initialize plotly (right column)
        const plotyDiv = document.getElementById('ploty_div');
        const data = []  // data will be update by `updatePlot()`
        const layout = this.getPlotlyLayout();
        Plotly.newPlot(plotyDiv, data, layout);
    },
    initializeTargetVarsUI() {
        // populate the target variable select
        const $targetVarsSelect = $("#target_variable");
        const thisState = this.state;
        $targetVarsSelect.empty();
        this.state.target_variables.forEach(function (targetVar) {
            const selected = targetVar.value === thisState.selected_target_var ? 'selected' : '';
            const optionNode = `<option value="${targetVar.value}" ${selected} >${targetVar.text}</option>`;
            $targetVarsSelect.append(optionNode);
        });
    },
    initializeUnitsUI() {
        // populate the location select
        const $locationSelect = $("#unit");
        const thisState = this.state;
        $locationSelect.empty();
        this.state.locations.forEach(function (location) {
            const selected = location.value === thisState.selected_location ? 'selected' : '';
            const optionNode = `<option value="${location.value}" ${selected} >${location.text}</option>`;
            $locationSelect.append(optionNode);
        });
    },
    initializeIntervalsUI() {
        // populate the interval select
        const $intervalsSelect = $("#intervals");
        const thisState = this.state;
        $intervalsSelect.empty();
        this.state.intervals.forEach(function (interval) {
            const selected = interval === thisState.selected_interval ? 'selected' : '';
            const optionNode = `<option value="${interval}" ${selected} >${interval}</option>`;
            $intervalsSelect.append(optionNode);
        });
    },
    updateModelsList() {
        // populate the select model div
        const $selectModelDiv = $("#forecastViz_select_model");
        const thisState = this.state;
        $selectModelDiv.empty();

        // split models into two groups: those with forecasts (enabled, colored) and those without (disabled, gray)
        // 1. add models with forecasts
        this.state.models
            .filter(function (model) {
                return App.state.forecasts.hasOwnProperty(model);
            })
            .forEach(function (model, modelIdx) {
                const isChecked = (thisState.selected_models.indexOf(model) > -1);
                $selectModelDiv.append(_selectModelDiv(model, thisState.colors[modelIdx], true, isChecked));
            });

        // 2. add models without forecasts
        this.state.models
            .filter(function (model) {
                return !App.state.forecasts.hasOwnProperty(model);
            })
            .forEach(function (model) {
                $selectModelDiv.append(_selectModelDiv(model, 'grey', false, false));
            });

        // re-wire up model checkboxes
        this.addModelCheckEventHandler();
    },
    addEventHandlers() {
        // option, location, and interval selects
        $('#target_variable').on('change', function () {
            App.state.selected_target_var = this.value;
            App.fetchDataUpdatePlot(true, true);
        });
        $('#location').on('change', function () {
            App.state.selected_location = this.value;
            App.fetchDataUpdatePlot(true, true);
        });
        $('#intervals').on('change', function () {
            App.state.selected_interval = this.value;
            App.fetchDataUpdatePlot(false, null);
        });

        // truth checkboxes
        $("#forecastViz_Current_Truth").change(function () {
            _setSelectedTruths();
        });
        $("#forecastViz_Truth_as_of").change(function () {
            _setSelectedTruths();
        });

        // Shuffle Colours button
        $("#forecastViz_shuffle").click(function () {
            App.state.colors = App.state.colors.sort(() => 0.5 - Math.random())
            App.updateModelsList();
            App.updatePlot();
        });

        // "Select Models" checkbox
        $("#forecastViz_all").change(function () {
            const $this = $(this);
            const isChecked = $this.prop('checked');
            if (isChecked) {
                App.state.last_selected_models = App.state.selected_models;
                App.state.selected_models = App.selectableModels();
            } else {
                App.state.selected_models = App.state.last_selected_models;
            }
            App.checkModels(App.state.selected_models);
            App.updatePlot();
        });

        // wire up model checkboxes
        this.addModelCheckEventHandler();

        // left and right buttons
        $("#decrement_as_of").click(function () {
            App.decrementAsOf();
        });
        $("#increment_as_of").click(function () {
            App.incrementAsOf();
        });

        // left and right keys
        window.addEventListener('keydown', function (event) {
            if (event.code === "ArrowLeft") {
                App.decrementAsOf();
            } else if (event.code === "ArrowRight") {
                App.incrementAsOf();
            }
        });
    },
    addModelCheckEventHandler() {
        $(".model-check").change(function () {
            const $this = $(this);
            const model = $this.prop('id');
            const isChecked = $this.prop('checked');
            const isInSelectedModels = (App.state.selected_models.indexOf(model) > -1);
            if (isChecked && !isInSelectedModels) {
                App.state.selected_models.push(model);
            } else if (!isChecked && isInSelectedModels) {
                App.state.selected_models = App.state.selected_models.filter(function (value) {
                    return value !== model;
                });  // App.state.selected_models.remove(model);
            }
            App.fetchDataUpdatePlot(false, null);
        });
    },


    //
    // event handler functions
    //

    incrementAsOf() {
        const state = this.state;
        const as_of_index = state.available_as_ofs[state.selected_target_var].indexOf(state.selected_as_of_date);
        if (as_of_index < state.available_as_ofs[state.selected_target_var].length - 1) {
            state.selected_as_of_date = state.available_as_ofs[state.selected_target_var][as_of_index + 1];
            this.fetchDataUpdatePlot(true, false);
            this.updateTruthAsOfCheckboxText();
        }
    },
    decrementAsOf() {
        const state = this.state;
        const as_of_index = state.available_as_ofs[state.selected_target_var].indexOf(state.selected_as_of_date);
        if (as_of_index > 0) {
            state.selected_as_of_date = state.available_as_ofs[state.selected_target_var][as_of_index - 1];
            this.fetchDataUpdatePlot(true, false);
            this.updateTruthAsOfCheckboxText();
        }
    },
    updateTruthAsOfCheckboxText() {
        $("#asOfTruthDate").text(`As of ${this.state.selected_as_of_date}`);
    },

    // Returns an array of models that are not grayed out.
    selectableModels() {
        return App.state.models.filter(function (element, index) {
            return index < 100;
        });
    },

    // Checks each item in #forecastViz_select_model that's in the passed list.
    checkModels(models) {
        this.state.models.forEach(function (model) {
            const isShouldCheck = (models.indexOf(model) > -1);
            const $modelCheckbox = $(`#${model}`);
            $modelCheckbox.prop('checked', isShouldCheck);
        });
    },

    //
    // date fetch-related functions
    //

    /**
     * Updates the plot, optionally first fetching data.
     *
     * @param isFetchFirst true if should fetch before plotting. false if no fetch
     * @param isFetchCurrentTruth applies if isFetchFirst: controls whether current truth is fetched in addition to
     *  as_of truth and forecasts. ignored if not isFetchFirst
     */
    fetchDataUpdatePlot(isFetchFirst, isFetchCurrentTruth) {
        if (isFetchFirst) {
            const promises = [this.fetchAsOfTruth(), this.fetchForecasts()];
            if (isFetchCurrentTruth) {
                promises.push(this.fetchCurrentTruth());
            }
            console.log(`fetchDataUpdatePlot(${isFetchFirst}, ${isFetchCurrentTruth}): waiting on promises`);
            const $plotyDiv = $('#ploty_div');
            $plotyDiv.fadeTo(0, 0.5);
            Promise.all(promises).then((values) => {
                console.log(`fetchDataUpdatePlot(${isFetchFirst}, ${isFetchCurrentTruth}): Promise.all() done. updating plot`, values);
                this.updateModelsList();
                this.updatePlot();
                $plotyDiv.fadeTo(0, 1.0);
            });
        } else {
            console.log(`fetchDataUpdatePlot(${isFetchFirst}, ${isFetchCurrentTruth}): updating plot`);
            this.updateModelsList();
            this.updatePlot();
        }
    },
    fetchCurrentTruth() {
        return _fetchData(false,
            App.state.selected_target_var, App.state.selected_location, App.state.current_date)
            .then(response => response.json())
            .then((data) => {
                App.state.current_truth = data;
            });  // Promise
    },
    fetchAsOfTruth() {
        return _fetchData(false,
            App.state.selected_target_var, App.state.selected_location, App.state.selected_as_of_date)
            .then(response => response.json())
            .then((data) => {
                App.state.as_of_truth = data;
            });  // Promise
    },
    fetchForecasts() {
        return _fetchData(true,
            App.state.selected_target_var, App.state.selected_location, App.state.selected_as_of_date)
            .then(response => response.json())
            .then((data) => {
                App.state.forecasts = data;
            });  // Promise
    },


    //
    // plot-related functions
    //

    updatePlot() {
        const plotyDiv = document.getElementById('ploty_div');
        const data = this.getPlotlyData();
        let layout = this.getPlotlyLayout();

        /*
        const debugObj = {
            'selection': {
                'selected_target_var': this.state.selected_target_var,
                'selected_location': this.state.selected_location,
                'selected_interval': this.state.selected_interval,
                'selected_as_of_date': this.state.selected_as_of_date,
                'selected_truth': this.state.selected_truth,
                'selected_mo1dels': this.state.selected_models
            },
            'data': {
                'current_truth': this.state.current_truth,
                'as_of_truth': this.state.as_of_truth,
                'forecasts': this.state.forecasts
            },
            'plotly': {
                'data': data,
                'layout': layout,
            },
        };
        console.log('updatePlot()', JSON.stringify(debugObj));
        */

        if (data.length === 0) {
            layout = {title: {text: 'No Visualization Data Found'}};
        }
        Plotly.react(plotyDiv, data, layout);
    },
    getPlotlyLayout() {
        if (this.state.target_variables.length === 0) {
            return {};
        }

        const variable = this.state.target_variables.filter((obj) => obj.value === this.state.selected_target_var)[0].plot_text;
        const location = this.state.locations.filter((obj) => obj.value === this.state.selected_location)[0].text;
        return {
            autosize: true,
            showlegend: false,
            title: {
                text: `Forecasts of ${variable} <br> in ${location} as of ${this.state.selected_as_of_date}`,
                x: 0.5,
                y: 0.90,
                xanchor: 'center',
                yanchor: 'top',
            },
            xaxis: {
                title: {text: 'Date'}
            },
            yaxis: {
                title: {text: variable, hoverformat: '.2f'}
            }
        }
    },
    getPlotlyData() {
        const state = this.state;
        let pd = [];
        if (state.selected_truth.includes('Current Truth') && Object.keys(state.current_truth).length !== 0) {
            pd.push({
                x: state.current_truth.date,
                y: state.current_truth.y,
                type: 'scatter',
                mode: 'lines',
                name: 'Current Truth',
                marker: {color: 'darkgray'}
            })
        }
        if (state.selected_truth.includes('Truth as of') && Object.keys(state.as_of_truth).length !== 0) {
            pd.push({
                x: state.as_of_truth.date,
                y: state.as_of_truth.y,
                type: 'scatter',
                mode: 'lines',
                opacity: 0.5,
                name: `Truth as of ${state.selected_as_of_date}`,
                marker: {color: 'black'}
            })
        }

        let pd0 = []
        if (state.forecasts.length !== 0) {

            // add the line for predictive medians
            pd0 = Object.keys(state.forecasts).map((model) => {
                if (state.selected_models.includes(model)) {
                    const index = state.models.indexOf(model)
                    const model_forecasts = state.forecasts[model]
                    const date = model_forecasts.target_end_date
                    const lq1 = model_forecasts['q0.025']
                    const lq2 = model_forecasts['q0.25']
                    const mid = model_forecasts['q0.5']
                    const uq1 = model_forecasts['q0.75']
                    const uq2 = model_forecasts['q0.975']

                    // 1-3: sort model forecasts in order of target end date
                    // 1) combine the arrays:
                    const list = []
                    for (let j = 0; j < date.length; j++) {
                        list.push({
                            date: date[j],
                            lq1: lq1[j],
                            lq2: lq2[j],
                            uq1: uq1[j],
                            uq2: uq2[j],
                            mid: mid[j]
                        })
                    }

                    // 2) sort:
                    list.sort((a, b) => (moment(a.date).isBefore(b.date) ? -1 : 1))

                    // 3) separate them back out:
                    for (let k = 0; k < list.length; k++) {
                        model_forecasts.target_end_date[k] = list[k].date
                        model_forecasts['q0.025'][k] = list[k].lq1
                        model_forecasts['q0.25'][k] = list[k].lq2
                        model_forecasts['q0.5'][k] = list[k].mid
                        model_forecasts['q0.75'][k] = list[k].uq1
                        model_forecasts['q0.975'][k] = list[k].uq2
                    }

                    const x = [];
                    if (Object.keys(state.as_of_truth).length !== 0) {
                        x.push(state.as_of_truth.date.slice(-1)[0]);
                    }
                    x.push(model_forecasts.target_end_date.slice(0)[0]);

                    const y = [];
                    if (Object.keys(state.as_of_truth).length !== 0) {
                        y.push(state.as_of_truth.y.slice(-1)[0]);
                    }
                    y.push(model_forecasts['q0.5'].slice(0)[0]);

                    return {
                        x: x,
                        y: y,

                        mode: 'lines',
                        type: 'scatter',
                        name: model,
                        hovermode: false,
                        opacity: 0.7,
                        line: {color: state.colors[index]},
                        hoverinfo: 'none'
                    };
                }
                return []
            })
        }
        pd = pd.concat(...pd0)

        // add interval polygons
        let pd1 = []
        if (state.forecasts.length !== 0) {
            pd1 = Object.keys(state.forecasts).map((model) => {  // notes that state.forecasts are still sorted
                if (state.selected_models.includes(model)) {
                    const index = state.models.indexOf(model)
                    const is_hosp = state.selected_target_var === 'hosp'
                    const mode = is_hosp ? 'lines' : 'lines+markers'
                    const model_forecasts = state.forecasts[model]
                    let upper_quantile
                    let lower_quantile
                    const plot_line = {
                        // point forecast
                        x: model_forecasts.target_end_date,
                        y: model_forecasts['q0.5'],
                        type: 'scatter',
                        name: model,
                        opacity: 0.7,
                        mode,
                        line: {color: state.colors[index]}
                    }

                    if (state.selected_interval === '50%') {
                        lower_quantile = 'q0.25'
                        upper_quantile = 'q0.75'
                    } else if (state.selected_interval === '95%') {
                        lower_quantile = 'q0.025'
                        upper_quantile = 'q0.975'
                    } else {
                        return [plot_line]
                    }

                    const x = Object.keys(state.as_of_truth).length !== 0 ?
                        state.as_of_truth.date.slice(-1).concat(model_forecasts.target_end_date) :
                        model_forecasts.target_end_date;
                    const y1 = Object.keys(state.as_of_truth).length !== 0 ?
                        state.as_of_truth.y.slice(-1).concat(model_forecasts[lower_quantile]) :  // lower edge
                        model_forecasts[lower_quantile];
                    const y2 = Object.keys(state.as_of_truth).length !== 0 ?
                        state.as_of_truth.y.slice(-1).concat(model_forecasts[upper_quantile]) :
                        model_forecasts[upper_quantile];  // upper edge
                    return [
                        plot_line,
                        {
                            // interval forecast -- currently fixed at 50%
                            x: [].concat(x, x.slice().reverse()),
                            y: [].concat(y1, y2.slice().reverse()),
                            fill: 'toself',
                            fillcolor: state.colors[index],
                            opacity: 0.3,
                            line: {color: 'transparent'},
                            type: 'scatter',
                            name: model,
                            showlegend: false,
                            hoverinfo: 'skip'
                        }
                    ]
                }
                return []
            })
        }
        pd = pd.concat(...pd1)

        // done!
        return pd
    },
};
