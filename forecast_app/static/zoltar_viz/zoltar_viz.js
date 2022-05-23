function _selectModelDiv(model, modelIdx, modelColor) {
    return `
                <div class="form-group form-check"
                     style="margin-bottom: 0${modelIdx >= 100 ? '; color: lightgrey' : ''}">
                    <label>
                        <input type="checkbox" id="${model}" ${modelIdx >= 100 ? 'disabled="disabled"' : ''}>
                        &nbsp;${model}
                        &nbsp;<span class="forecastViz_dot" style="background-color:
                            ${modelIdx < 100 ? modelColor : 'lightgray'}; "></span>
                    </label>
                </div>
                `;
}

// set up straightforward state via app concept - based on https://dev.to/vijaypushkin/dead-simple-state-management-in-vanilla-javascript-24p0
const App = {

    projectId: -1,  // set by initialize()
    csrf_token: '',  // ""

    state: {
        // Static data, fixed at time of creation
        target_variables: [],
        locations: [],
        intervals: [],
        available_as_ofs: [],
        current_date: "",
        models: [],
        default_models: [],
        all_models: false,
        disclaimer: "",

        // Dynamic/updated and we need to track: 2 categories:
        // 1/2 Tracks UI state:
        target_var: '',
        location: '',
        interval: '',
        as_of_date: '',
        data: ['Current Truth', 'Truth as of'],
        current_models: [],
        colors: [],

        // 2/2 Data used to create plots:
        as_of_truth: [],
        current_truth: [],
        forecasts: {},
    },
    initialize(projectId, csrf_token) {
        App.projectId = projectId;
        App.csrf_token = csrf_token;

        /*
        // todo xx this all needs careful thinking:
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

        this.initStaticVars();

        // initialize plotly
        const ploty_div = document.getElementById('ploty_div');
        const data = []  // todo xx [{x: [1, 2, 3, 4, 5], y: [1, 2, 4, 8, 16]}]
        const layout = {margin: {t: 0}};  // todo xx
        Plotly.newPlot(ploty_div, data, layout);

        // wire up UI controls
        // todo xx document.getElementById("button").addEventListener("click", () => this.increment());#}

        // todo xx sync state -> UI

    },
    initStaticVars() {
        // this.state.target_variables
        $.ajax({
            url: "/api/project/" + App.projectId + "/viz-target-vars/",
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                App.setTargetVars(data);
            },
            error: function (jqXHR, textStatus, thrownError) {
                console.log("initStaticVars(): error(): target_variables", textStatus, thrownError);
            }
        });

        // this.state.locations
        $.ajax({
            url: "/api/project/" + App.projectId + "/viz-units/",
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                App.setLocations(data);
            },
            error: function (jqXHR, textStatus, thrownError) {
                console.log("initStaticVars(): error(): locations", textStatus, thrownError);
            }
        });

        // this.state.intervals
        this.state.intervals = ['0%', '50%', '95%']  // todo xx

        // this.state.available_as_ofs
        $.ajax({
            url: "/api/project/" + App.projectId + "/viz-avail-ref-dates/",
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                App.setAvailableAsOfs(data);
            },
            error: function (jqXHR, textStatus, thrownError) {
                console.log("initStaticVars(): error(): as-ofs", textStatus, thrownError);
            }
        });

        // this.state.current_date
        // todo xx

        // this.state.models
        $.ajax({
            url: "/api/project/" + App.projectId + "/viz-models/",
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                App.setModels(data);
            },
            error: function (jqXHR, textStatus, thrownError) {
                console.log("initStaticVars(): error(): models", textStatus, thrownError);
            }
        });

        // this.state.default_models
        // todo xx

        // this.state.all_models
        // todo xx

        // this.state.disclaimer
        // todo xx
    },
    setModels(models) {
        this.state.models = models;
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

        // update the select model div. todo xx hard-coded ID
        const $selectModelDiv = $("#forecastViz_select_model");
        $selectModelDiv.empty();
        const thisState = this.state;
        this.state.models.forEach(function (model, modelIdx) {
            $selectModelDiv.append(_selectModelDiv(model, modelIdx, thisState.colors[modelIdx]));
        });

        // todo xx update other UI
    },
    setTargetVars(targetVars) {
        this.state.target_variables = targetVars;

        // update the target variable select. todo xx hard-coded ID
        const $targetVarsSelect = $("#target_variable");
        $targetVarsSelect.empty();
        this.state.target_variables.forEach(function (targetVar) {
            const optionNode = `<option value="${targetVar.value}">${targetVar.text}</option>`;
            $targetVarsSelect.append(optionNode);
        });

        // todo xx update other UI
    },
    setLocations(locations) {
        this.state.locations = locations;

        // update the location select. todo xx hard-coded ID
        const $locationSelect = $("#location");
        $locationSelect.empty();
        this.state.locations.forEach(function (location) {
            const optionNode = `<option value="${location.value}">${location.text}</option>`;
            $locationSelect.append(optionNode);
        });

        // todo xx update other UI
    },
    setAvailableAsOfs(availableAsOfs) {
        this.state.available_as_ofs = availableAsOfs;

        // todo xx update other UI
    }
};
