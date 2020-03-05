// Script for generating influenza truth file similar to target-multival here
// https://github.com/FluSightNetwork/cdc-flusight-ensemble/blob/master/scores/target-multivals.csv

// Install dependencies using `npm i`
// Run `node index.js` to generate output `truths.csv`

const fct = require('flusight-csv-tools')
const fs = require('fs-extra')
const mmwr = require('mmwr-week')

// A season 20xx-20yy is represented using just the first year 20xx
const SEASONS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019] // for the Flusight Ensemble
// const SEASONS = [2017]  // for the lab's 2017-2018 season
// const SEASONS = [2016]  // "" 2016-2017 season

const HEADERS = 'timezero,unit,target,value'

const OUTPUT_FILE = 'truths-2010-through-2018.csv' // originally 'truths.csv'
// const OUTPUT_FILE = 'truths-2017-2018-reichlab.csv'
// const OUTPUT_FILE = 'truths-2016-2017-reichlab.csv'

function epiweekToTimezero(ew) {
  let mdate = new mmwr.MMWRDate()
  mdate.fromEpiweek(ew)
  return mdate.toMomentDate().add(1, 'days').format('YYYY-MM-DD')
}

async function main() {
  let allTruth = await Promise.all(SEASONS.map(s => fct.truth.getSeasonTruth(s)))

  let rows = [].concat(...allTruth.map(seasonTruth => {
    return [].concat(...fct.meta.regionIds.map(regionId => {
      return [].concat(...seasonTruth[regionId].map(truth => {
        return fct.meta.targetIds.map(target => {
          let value = truth[target] === null ? 'NULL' : truth[target]

          if ((target === 'onset-wk') || (target === 'peak-wk')) {
            // Convert epiweek type truths to timezero style stamps
            if (value !== 'NULL') {
              value = epiweekToTimezero(value)
            }
          }

          return `${epiweekToTimezero(truth.epiweek)},${fct.meta.regionFullName[regionId]},${fct.meta.targetFullName[target]},${value}`
        })
      }))
    }))
  }))

  await fs.writeFile(OUTPUT_FILE, HEADERS + '\n' + rows.join('\n'))
}

main()
  .then(() => console.log('All done'))
  .catch(e => {
    console.log(e)
    process.exit(1)
  })