Welcome to your new dbt project!

### Using the starter project

Try running the following commands:

- Build docker for dbt+fal: docker build -f Dockerfile-fal -t fal:latest .
- Run debug to make sure our settings are correct: docker run --rm -it -v /Users/macbookpro/code/amazon_custom_dbt_transformation:/amz -w /amz --network host -t fal:latest debug --profiles-dir=. --project-dir=.
- Run transformation: docker run --rm -it -v /Users/macbookpro/code/amazon_custom_dbt_transformation:/amz -w /amz --network host -t fal:latest run --profiles-dir=. --project-dir=.

### Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
