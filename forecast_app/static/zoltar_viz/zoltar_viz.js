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


/**
 * `initialize()` helper that builds UI by adding DOM elements to $componentDiv. the UI is one row with two columns:
 * options on left and the plotly plot on the right
 *
 * @param $componentDiv - an empty Bootstrap 4 row (JQuery object)
 * @private
 */
function _createUIElements($componentDiv) {
    //
    // make $optionsDiv (left column)
    //
    const $optionsDiv = $('<div class="col-md-3"></div>');

    // add Outcome, Unit, and Interval selects (form)
    const $outcomeFormRow = $(
        '<div class="form-row">\n' +
        '    <label for="target_variable" class="col-sm-4 col-form-label">Outcome:</label>\n' +
        '    <div class="col-sm-8">\n' +
        '        <select id="target_variable" class="form-control" name="target_variables"></select>\n' +
        '    </div>\n' +
        '</div>');
    const $unitFormRow = $(
        '<div class="form-row">\n' +
        '    <label for="unit" class="col-sm-4 col-form-label">Unit:</label>\n' +
        '    <div class="col-sm-8">\n' +
        '        <select id="unit" class="form-control" name="unit"></select>\n' +
        '    </div>\n' +
        '</div>');
    const $intervalFormRow = $(
        '<div class="form-row">\n' +
        '    <label for="intervals" class="col-sm-4 col-form-label">Interval:</label>\n' +
        '    <div class="col-sm-8">\n' +
        '        <select id="intervals" class="form-control" name="intervals">\n' +
        '    </div>\n' +
        '</div>');
    const $optionsForm = $('<form></form>').append($outcomeFormRow, $unitFormRow, $intervalFormRow);
    $optionsDiv.append($optionsForm);

    // add truth checkboxes
    const $truthCheckboxesDiv = $(
        '<div class="form-group form-check forecastViz_select_data ">\n' +
        '    <input title="curr truth" type="checkbox" id="forecastViz_Current_Truth" value="Current Truth" checked>\n' +
        '      &nbsp;<span id="currentTruthDate">Current ({xx_current_date})</span>\n' +
        '      &nbsp;<span class="forecastViz_dot" style="background-color: lightgrey; "></span>\n' +
        '    <br>\n' +
        '    <input title="truth as of" type="checkbox" id="forecastViz_Truth_as_of" value="Truth as of" checked>\n' +
        '      &nbsp;<span id="asOfTruthDate">As of {xx_as_of_date}</span>\n' +
        '      &nbsp;<span class="forecastViz_dot" style="background-color: black;"></span>\n' +
        '</div>');
    $optionsDiv.append('<div class="pt-md-3">Select Truth Data:</div>');
    $optionsDiv.append($truthCheckboxesDiv);

    // add model list controls
    $optionsDiv.append($(
        '<button type="button" class="btn btn-sm rounded-pill" id="forecastViz_shuffle" style="float: right;">\n' +
        '    Shuffle Colours</button>\n' +
        '<label class="forecastViz_label" for="forecastViz_all">Select Models:</label>\n' +
        '<input type="checkbox" id="forecastViz_all">'));

    // add the model list itself
    $optionsDiv.append($('<div id="forecastViz_select_model"></div>'));


    //
    // make $vizDiv (right column)
    //
    const $vizDiv = $('<div class="col-md-9"></div>');
    const $buttonsDiv = $(
        '<div class="container">\n' +
        '    <div class="col-md-12 text-center">\n' +
        '        <button type="button" class="btn btn-primary" id="decrement_as_of">&lt;</button>\n' +
        '        <button type="button" class="btn btn-primary" id="increment_as_of">&gt;</button>\n' +
        '    </div>\n' +
        '</div>'
    );
    $vizDiv.append($('<p class="forecastViz_disclaimer"><b><span id="disclaimer">{xx_disclaimer}</span></b></p>'));
    $vizDiv.append($('<div id="ploty_div"></div>'));
    $vizDiv.append($buttonsDiv);
    $vizDiv.append($('<p style="text-align:center"><small>Note: You can navigate to forecasts from previous weeks with the left and right arrow keys</small></p>'));


    //
    // finish
    //
    $componentDiv.empty().append($optionsDiv, $vizDiv);
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
        // _fetchData that was passed to initialize():
        _fetchData: null,


        // Static data, fixed at time of creation:
        target_variables: [],
        units: [],
        intervals: [],
        available_as_ofs: [],
        current_date: "",
        models: [],
        initial_checked_models: [],
        disclaimer: "",

        // Dynamic/updated and we need to track: 2 categories:
        // 1/2 Tracks UI state:
        selected_target_var: '',
        selected_unit: '',
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

    /**
     * Toggle visibility of a content tab
     * @param {String} componentDiv - id of a DOM node to populate. it must be an empty Bootstrap 4 row
     * @param {String} _fetchData - function as documented in forecast-repository/forecast_app/templates/project_viz.html .
     *                              args: isForecast, targetKey, unitAbbrev, referenceDate
     * @param {int} options - visualization initialization options as documented at https://docs.zoltardata.com/visualizationoptionspage/
     */
    initialize(componentDiv, _fetchData, options) {
        App._fetchData = _fetchData;
        console.log('initialize(): entered', _fetchData);

        // save static vars
        this.state.target_variables = options['target_variables'];
        this.state.units = options['units'];
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
        this.state.selected_unit = options['initial_unit'];
        this.state.selected_interval = options['init_interval'];
        this.state.selected_as_of_date = options['current_date'];
        // this.state.selected_truth: synchronized via default <input ... checked> setting
        this.state.selected_models = options['initial_checked_models'];

        /*
        const debugObj = {
            'target_variables': this.state.target_variables,
            'units': this.state.units,
            'intervals': this.state.intervals,
            'available_as_ofs': this.state.available_as_ofs,
            'current_date': this.state.current_date,
            'models': this.state.models,
            'colors': this.state.colors,
        };
        console.log('initialize(): static vars initialized', JSON.stringify(debugObj));
        */

        // populate UI elements, setting selection state to initial, first validating `componentDiv`
        const componentDivEle = document.getElementById(componentDiv);
        if (componentDivEle === null) {
            throw `componentDiv DOM node not found: '${componentDiv}'`;
        }

        console.log('initialize(): initializing UI');
        const $componentDiv = $(componentDivEle);
        _createUIElements($componentDiv);
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
        // populate the unit select
        const $unitSelect = $("#unit");
        const thisState = this.state;
        $unitSelect.empty();
        this.state.units.forEach(function (unit) {
            const selected = unit.value === thisState.selected_unit ? 'selected' : '';
            const optionNode = `<option value="${unit.value}" ${selected} >${unit.text}</option>`;
            $unitSelect.append(optionNode);
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
            .forEach(function (model) {
                const isChecked = (thisState.selected_models.indexOf(model) > -1);
                const modelIdx = thisState.models.indexOf(model);
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
        // option, unit, and interval selects
        $('#target_variable').on('change', function () {
            App.state.selected_target_var = this.value;
            App.fetchDataUpdatePlot(true, true);
        });
        $('#unit').on('change', function () {
            App.state.selected_unit = this.value;
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
        return App._fetchData(false,
            App.state.selected_target_var, App.state.selected_unit, App.state.current_date)
            .then(response => response.json())
            .then((data) => {
                App.state.current_truth = data;
            });  // Promise
    },
    fetchAsOfTruth() {
        return App._fetchData(false,
            App.state.selected_target_var, App.state.selected_unit, App.state.selected_as_of_date)
            .then(response => response.json())
            .then((data) => {
                App.state.as_of_truth = data;
            });  // Promise
    },
    fetchForecasts() {
        return App._fetchData(true,
            App.state.selected_target_var, App.state.selected_unit, App.state.selected_as_of_date)
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
                'selected_unit': this.state.selected_unit,
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
        const unit = this.state.units.filter((obj) => obj.value === this.state.selected_unit)[0].text;
        return {
            autosize: true,
            showlegend: false,
            title: {
                text: `Forecasts of ${variable} <br> in ${unit} as of ${this.state.selected_as_of_date}`,
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
