from sys import path
import settings

# make the working version modules prior to the installed one.
path.insert(1, settings.testing_subject_path)
