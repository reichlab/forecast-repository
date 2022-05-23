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
        console.log("initialize(): entered", projectId, csrf_token)
        App.projectId = projectId;
        App.csrf_token = csrf_token;

        // configure AJAX to work with DRF - per https://stackoverflow.com/questions/42514560/django-and-ajax-csrf-token-missing-despite-being-passed
        function csrfSafeMethod(method) {
            return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
        }

        $.ajaxSetup({
            beforeSend: function (xhr, settings) {
                if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
                    xhr.setRequestHeader("X-CSRFToken", '{{ csrf_token }}');
                }
            }
        });

        // init static information
        this.init_static_vars();
        App.initColors();

        // initialize plotly
        const ploty_div = document.getElementById('ploty_div');
        const data = []  // todo xx [{x: [1, 2, 3, 4, 5], y: [1, 2, 4, 8, 16]}]
        const layout = {margin: {t: 0}};  // todo xx
        Plotly.newPlot(ploty_div, data, layout);

        // wire up UI controls
        // todo xx document.getElementById("button").addEventListener("click", () => this.increment());#}

        // todo xx sync state -> UI

        console.log("initialize(): done")
    },
    init_static_vars() {
        console.log("init_static_vars(): entered")

        // this.state.target_variables
        // todo xx

        // this.state.locations
        // todo xx

        // this.state.intervals
        this.state.intervals = ['0%', '50%', '95%']  // todo xx

        // this.state.available_as_ofs
        // todo xx

        // this.state.current_date
        // todo xx

        // this.state.models
        const url = "/api/project/" + App.projectId + "/viz-models/"
        console.log("init_static_vars(): calling ajax: url=" + url)
        $.ajax({
            url: url,
            type: 'GET',
            dataType: 'json',
            success: function (data, textStatus, jqXHR) {
                // console.log("init_static_vars(): success(): data=" + data + ", textStatus=" + textStatus + ", jqXHR=" + jqXHR);
                App.set_models(data);
            },
            error: function (jqXHR, textStatus, thrownError) {
                console.log("init_static_vars(): error(): textStatus=" + textStatus + ", thrownError=" + thrownError);
            }
        });

        // this.state.default_models
        // todo xx

        // this.state.all_models
        // todo xx

        // this.state.disclaimer
        // todo xx

        console.log("init_static_vars(): done")
    },
    initColors() {
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
    },
    set_models(models) {
        this.state.models = models;

        // update the select model div. todo xx hard-coded ID
        const $selectModelDiv = $("#forecastViz_select_model");
        $selectModelDiv.empty();
        const thisState = this.state;
        this.state.models.forEach(function (model, modelIdx) {
            const modelColor = thisState.colors[modelIdx];
            $selectModelDiv.append(_selectModelDiv(model, modelIdx, modelColor));
        });

        // todo xx update other UI

    },
    increment_as_of() {
        // todo xx
    },
    decrement_as_of() {
        // todo xx
    }
};
