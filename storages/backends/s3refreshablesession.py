from uuid import uuid4
from datetime import datetime
from time import time

import boto3
from boto3 import Session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from botocore.credentials import InstanceMetadataFetcher
from storages.utils import setting
import logging 

class InstanceMetadataBotoSession:
    METHOD = 'iam-role'
    CANONICAL_NAME = 'Ec2InstanceMetadata'
    
    """
    Boto Helper class which lets us create refreshable session, so that we can cache the client or resource.

    Usage
    -----
    session = BotoSession().refreshable_session()

    client = session.client("s3") # we now can cache this client object without worrying about expiring credentials
    """

    def __init__(
        self,
        region_name: str = None,
        session_name: str = None,
    ):
        """
        Initialize `BotoSession`

        Parameters
        ----------
        region_name : str (optional)
            Default region when creating new connection.

        session_name : str (optional)
            An identifier for the assumed role session. (required when `sts_arn` is given)
        """
        self.region_name = region_name

        # read why RoleSessionName is important https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html
        self.session_name = session_name or uuid4().hex
        self._role_fetcher = InstanceMetadataFetcher(timeout=setting("S3_CREDENTIALS_TIMEOUT", 1000), num_attempts=3)

        self.access_key = None 
        self.secret_key = None 

    def __get_session_credentials(self):
        """
        Get session credentials
        """
        fetcher = self._role_fetcher
        # We do the first request, to see if we get useful data back.
        # If not, we'll pass & move on to whatever's next in the credential
        # chain.
        metadata = fetcher.retrieve_iam_role_credentials()
        if not metadata:
            return None
        logging.debug('Found credentials from IAM Role: %s',
                     metadata['role_name'])
        # We manually set the data here, since we already made the request &
        # have it. When the expiry is hit, the credentials will auto-refresh
        # themselves.
        credentials = RefreshableCredentials.create_from_metadata(
            metadata,
            method=self.METHOD,
            refresh_using=fetcher.retrieve_iam_role_credentials,
        )

        self.access_key = credentials.access_key
        self.secret_key = credentials.secret_key
        
        return credentials

    def refreshable_session(self) -> Session:
        """
        Get refreshable boto3 session.
        """
        try:
            # get refreshable credentials
            refreshable_credentials = RefreshableCredentials.create_from_metadata(
                metadata=self.__get_session_credentials(),
                refresh_using=self._role_fetcher.retrieve_iam_role_credentials,
                method=self.METHOD,
            )

            # attach refreshable credentials current session
            session = get_session()
            session._credentials = refreshable_credentials
            session.set_config_variable("region", self.region_name)
            autorefresh_session = Session(botocore_session=session)

            return autorefresh_session

        except:
            return boto3.session.Session()