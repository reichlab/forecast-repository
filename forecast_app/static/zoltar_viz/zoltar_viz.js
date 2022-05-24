//
// helper functions
//

// `initializeModelsUI()` helper
function _selectModelDiv(model, modelIdx, modelColor, isChecked) {
    const disabled = modelIdx >= 100 ? 'disabled="disabled"' : '';
    const checked = isChecked ? 'checked' : '';
    return `<div class="form-group form-check"
                 style="margin-bottom: 0${modelIdx >= 100 ? '; color: lightgrey' : ''}">
                <label>
                    <input type="checkbox" id="${model}" class="model-check" ${disabled} ${checked}>
                    &nbsp;${model}
                    &nbsp;<span class="forecastViz_dot" style="background-color:
                        ${modelIdx < 100 ? modelColor : 'lightgray'}; "></span>
                </label>
            </div>`;
}


// event handler helper
function _setSelectedTruths() {
    const isCurrTruthChecked = $("#forecastViz_Current_Truth").prop('checked');  // todo xx hard-coded ID
    const isAsOfTruthChecked = $("#forecastViz_Truth_as_of").prop('checked');  // ""
    const selectedTruths = [];
    if (isCurrTruthChecked) {
        selectedTruths.push('Current Truth');
    }
    if (isAsOfTruthChecked) {
        selectedTruths.push('Truth as of');
    }
    App.state.selected_truth = selectedTruths;
    // todo xx update dependencies
}


//
// App
//

// this implements a straightforward SPA with state - based on https://dev.to/vijaypushkin/dead-simple-state-management-in-vanilla-javascript-24p0
const App = {
    projectId: -1,  // set by initialize()
    state: {
        // Static data, fixed at time of creation
        target_variables: [],
        locations: [],
        intervals: [],
        available_as_ofs: [],
        current_date: "",
        models: [],
        default_models: [],
        // all_models: false,  // todo xx
        // disclaimer: "",  // todo xx

        // Dynamic/updated and we need to track: 2 categories:
        // 1/2 Tracks UI state:
        selected_target_var: '',
        selected_location: '',
        selected_interval: '',
        selected_as_of_date: '',
        selected_truth: ['Current Truth', 'Truth as of'],
        selected_models: [],
        colors: [],

        // 2/2 Data used to create plots:
        current_truth: [],
        as_of_truth: [],
        forecasts: {},
    },
    initialize(projectId, options) {
        App.projectId = projectId;

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
        this.state.default_models = options['default_models'];
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
        this.state.selected_target_var = options['init_target_var'];
        this.state.selected_location = options['init_unit'];
        this.state.selected_interval = options['init_interval'];
        this.state.selected_as_of_date = options['current_date'];
        // this.state.selected_truth: synchronized via default <input ... checked> setting
        this.state.selected_models = options['default_models'];

        // populate UI elements, setting selection state to initial
        this.initializeUI();

        // wire up UI controls (event handlers)
        this.addEventHandlers();

        // todo xx pull initial data (current truth, selected truth, and selected forecast)

    },
    addEventHandlers() {
        // option, location, and interval selects
        $('#target_variable').on('change', function () {  // todo xx hard-coded ID
            App.state.selected_target_var = this.value;
            // todo xx update dependencies
        });
        $('#location').on('change', function () {  // todo xx hard-coded ID
            App.state.selected_location = this.value;
            // todo xx update dependencies
        });
        $('#intervals').on('change', function () {  // todo xx hard-coded ID
            App.state.selected_interval = this.value;
            // todo xx update dependencies
        });

        // truth checkboxes
        $("#forecastViz_Current_Truth").change(function () {  // todo xx hard-coded ID
            _setSelectedTruths();
        });
        $("#forecastViz_Truth_as_of").change(function () {  // todo xx hard-coded ID
            _setSelectedTruths();
        });

        // model checkboxes
        $(".model-check").change(function () {  // todo xx hard-coded ID
            const $this = $(this);
            const model = $this.prop('id');
            const isChecked = $this.prop('checked');
            const isAlreadyInArray = (App.state.selected_models.indexOf(model) > -1);
            if (isChecked && !isAlreadyInArray) {
                App.state.selected_models.push(model);
            } else if (!isChecked && isAlreadyInArray) {
                // App.state.selected_models.remove(model);  // I wish
                App.state.selected_models = App.state.selected_models.filter(function (value, index, arr) {
                    return value !== model;
                });
            }
        });

        // left and right keys
        // todo xx

        // left and right buttons
        // todo xx

    },
    initializeUI() {
        // initialize options and models list (left column)
        App.initializeTargetVarsUI();
        App.initializeLocationsUI();
        App.initializeIntervalsUI();
        App.initializeModelsUI();

        // initialize plotly (right column)
        const ploty_div = document.getElementById('ploty_div');
        const data = []  // todo xx [{x: [1, 2, 3, 4, 5], y: [1, 2, 4, 8, 16]}]
        const layout = {margin: {t: 0}};  // todo xx
        Plotly.newPlot(ploty_div, data, layout);
    },
    initializeTargetVarsUI() {
        // update the target variable select
        const $targetVarsSelect = $("#target_variable");  // todo xx hard-coded ID
        const thisState = this.state;
        $targetVarsSelect.empty();
        this.state.target_variables.forEach(function (targetVar) {
            const selected = targetVar.value === thisState.selected_target_var ? 'selected' : '';
            const optionNode = `<option value="${targetVar.value}" ${selected} >${targetVar.text}</option>`;
            $targetVarsSelect.append(optionNode);
        });
    },
    initializeLocationsUI() {
        // update the location select
        const $locationSelect = $("#location");  // todo xx hard-coded ID
        const thisState = this.state;
        $locationSelect.empty();
        this.state.locations.forEach(function (location) {
            const selected = location.value === thisState.selected_location ? 'selected' : '';
            const optionNode = `<option value="${location.value}" ${selected} >${location.text}</option>`;
            $locationSelect.append(optionNode);
        });
    },
    initializeIntervalsUI() {
        // update the interval select
        const $intervalsSelect = $("#intervals");  // todo xx hard-coded ID
        const thisState = this.state;
        $intervalsSelect.empty();
        this.state.intervals.forEach(function (interval) {
            const selected = interval === thisState.selected_interval ? 'selected' : '';
            const optionNode = `<option value="${interval}" ${selected} >${interval}</option>`;
            $intervalsSelect.append(optionNode);
        });
    },
    initializeModelsUI() {
        // update the select model div
        const $selectModelDiv = $("#forecastViz_select_model");  // todo xx hard-coded ID
        const thisState = this.state;
        $selectModelDiv.empty();
        this.state.models.forEach(function (model, modelIdx) {
            const isChecked = (thisState.selected_models.indexOf(model) > -1);
            $selectModelDiv.append(_selectModelDiv(model, modelIdx, thisState.colors[modelIdx], isChecked));
        });
    },
};
