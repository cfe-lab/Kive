import { Point } from "../canvas/geometry";

export interface PipelineData {
    kive_version: string;
    default_config: PipelineConfig;
    steps?: Step[];
    inputs?: DataSource[];
    outputs?: DataSource[];
}
export interface Container {
    files: string[];
    pipeline: PipelineData;
}
export interface PipelineConfig {
    parent_family: string;
    parent_tag: string;
    parent_md5: string;
    memory: number;
    threads: number;
}

export interface DataSource {
    x?: number;
    y?: number;
    dataset_name: string;
    source_step?: number;
    source_dataset_name?: string;
}

interface Step extends Point {
    driver: string;
    fill_colour?: string;
    inputs: DataSource[];
    outputs: string[];
}
