#! /usr/bin/env python

from mpi4py import MPI
import fleet.workers


def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        manager = fleet.workers.Manager()
        manager.main_procedure()

    else:
        worker = fleet.workers.Worker()
        worker.main_procedure()


if __name__ == "__main__":
    main()