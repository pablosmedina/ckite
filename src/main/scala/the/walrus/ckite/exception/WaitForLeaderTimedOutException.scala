package the.walrus.ckite.exception

import java.util.concurrent.TimeoutException

class WaitForLeaderTimedOutException(exception: TimeoutException) extends RuntimeException(exception) {

}