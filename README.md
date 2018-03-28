# redis-leaderboards
Updates a Redis instance with four types of leaderboards that were derived from the test dataset from Sentiment140 (http://help.sentiment140.com/for-students/).

## Commands
Had Anaconda3 installed and set as my default Python executable.

### Conda Commands
```text
conda install redis-py
conda install coverage
```

### Script Commands
```text
python leaderboards.py --csv ~/testdata.manual.2009.06.14.csv --redis redis://localhost:6379/0

or

python leaderboards.py --csv ~/testdata.manual.2009.06.14.csv --redis redis://username:password@localhost:6379/0
```

### Test Commands
```text
nosetests --with-coverage --cover-html --cover-package=. test_leaderboards.py
```

## Sample Output
### Most Positive Leaderboard
Redis Key: most_positive
```json
[{
	"twitter_id": "MamiYessi",
	"counts": 2
}, {
	"twitter_id": "souleaterjh",
	"counts": 2
}, {
	"twitter_id": "A_TALL_BLONDE",
	"counts": 1
}]
```

### Most Negative Leaderboard
Redis Key: most_negative
```json
[{
	"twitter_id": "tradecruz",
	"counts": 2
}, {
	"twitter_id": "Adrigonzo",
	"counts": 1
}, {
	"twitter_id": "kerrrrrr",
	"counts": 1
}]
```

### Most Queried Leaderboard
Redis Key: most_queried
```json
[{
	"query": "time warner",
	"counts": 35
}, {
	"query": "nike",
	"counts": 25
}, {
	"query": "\"night at the museum\"",
	"counts": 25
}]
```

### Most Tweeted Leaderboard
Redis Key: most_tweeted
```json
[{
	"twitter_id": "vmkobs",
	"counts": 3
}, {
	"twitter_id": "cfbloggers",
	"counts": 2
}, {
	"twitter_id": "MamiYessi",
	"counts": 2
}]
```